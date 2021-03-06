#!/usr/bin/env python

"""
Processes all packages in a given suite to extract interesting metadata
(mainly AppStream metainfo data). The data will be stored in
the "bin_dep11" table.
Additionally, a screenshot cache and tarball of all the icons of packages
beloging to a given suite will be created.
"""

# Copyright (c) 2014 Abhishek Bhattacharjee <abhishek.bhattacharjee11@gmail.com>
# Copyright (c) 2014-2015 Matthias Klumpp <mak@debian.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

import sys
import tarfile
import shutil
import apt_pkg
import os
import yaml
import uuid
import glob

from find_metainfo import *
from dep11.extractor import MetadataExtractor
from dep11.component import DEP11Component, DEP11YamlDumper, get_dep11_header

from daklib import daklog
from daklib.daksubprocess import call, check_call
from daklib.filewriter import DEP11DataFileWriter, DEP11HintsFileWriter
from daklib.config import Config
from daklib.dbconn import *
from daklib.dakmultiprocessing import DakProcessPool, PROC_STATUS_SUCCESS, PROC_STATUS_SIGNALRAISED

def usage():
    print("""Usage: dak generate_metadata -s <suitename> [OPTION]
Extract DEP-11 metadata for the specified suite.

  -e, --expire       Clear the icon/screenshot cache from stale data.
  -h, --write-hints  Export YAML documents with issues found while processing the packages.
    """)

class MetadataPool:
    '''
    Keeps a pool of component metadata per arch per component
    '''

    def __init__(self, values):
        '''
        Initialize the metadata pool.
        '''
        self._values = values
        self._mcpts = dict()

    def append_cptdata(self, arch, cptlist):
        '''
        Makes a list of all the DEP11Component objects in a arch pool
        '''
        cpts = self._mcpts.get(arch)
        if not cpts:
            self._mcpts[arch] = list()
            cpts = self._mcpts[arch]
        for c in cptlist:
            # TODO: Maybe check for duplicates here?
            # Right now, we can easily filter them out later and complain about it at the maintainer side,
            # so a hard-check on duplicate ids might not be necessary.
            cpts.append(c)

    def export(self, session):
        """
        Saves metadata in db (serialized to YAML)
        """
        for arch, cpts in self._mcpts.items():
            values = self._values
            values['architecture'] = arch
            dep11 = DEP11Metadata(session)
            for cpt in cpts:
                # get the metadata in YAML format
                metadata = cpt.to_yaml_doc()
                hints_yml = cpt.get_hints_yaml()
                if not hints_yml:
                    hints_yml = ""

                # store metadata in database
                dep11.insert_data(cpt._binid, cpt.cid, metadata, hints_yml, cpt.has_ignore_reason())
        # commit all changes
        session.commit()

##############################################################################

def make_icon_tar(suitename, component):
    '''
     icons-%(component)_%(size).tar.gz of each Component.
    '''
    cnf = Config()
    sizes  = cnf.value_list('DEP11::IconSizes')
    for size in sizes:
        icon_location_glob = os.path.join (cnf["Dir::MetaInfo"], suitename,  component, "*", "icons", size, "*.*")
        tar_location = os.path.join (cnf["Dir::Root"], "dists", suitename, component)

        icon_tar_fname = os.path.join(tar_location, "icons-%s_%s.tar.gz" % (component, size))
        tar = tarfile.open(icon_tar_fname, "w:gz")

        for filename in glob.glob(icon_location_glob):
            icon_name = os.path.basename (filename)
            tar.add(filename,arcname=icon_name)

        tar.close()

def extract_metadata(mde, sn, pkgname, metainfo_files, binid, package_fname, arch):
    cpts = mde.process(pkgname, package_fname, metainfo_files, binid)

    data = dict()
    data['arch'] = arch
    data['cpts'] = cpts
    data['message'] = "Processed package: %s (%s/%s)" % (pkgname, sn, arch)
    return (PROC_STATUS_SUCCESS, data)

def process_suite(session, suite, logger, force=False):
    '''
    Extract new metadata for a given suite.
    '''
    path = Config()["Dir::Pool"]

    if suite.untouchable and not force:
        import daklib.utils
        daklib.utils.fubar("Refusing to touch %s (untouchable and not forced)" % suite.suite_name)
        return

    for component in [ c.component_name for c in suite.components ]:
        mif = MetaInfoFinder(session)
        pkglist = mif.find_meta_files(component=component, suitename=suite.suite_name)

        values = {
            'archive': suite.archive.path,
            'suite': suite.suite_name,
            'component': component,
        }

        pool = DakProcessPool()
        dpool = MetadataPool(values)

        def parse_results(message):
            # Split out into (code, msg)
            code, msg = message
            if code == PROC_STATUS_SUCCESS:
                # we abuse the message return value here...
                logger.log([msg['message']])
                dpool.append_cptdata(msg['arch'], msg['cpts'])
            elif code == PROC_STATUS_SIGNALRAISED:
                logger.log(['E: Subprocess recieved signal ', msg])
            else:
                logger.log(['E: ', msg])

        cnf = Config()
        iconf = IconFinder(suite.suite_name, component)
        mde = MetadataExtractor(suite.suite_name, component,
                        cnf["Dir::MetaInfo"],
                        cnf["DEP11::Url"],
                        cnf.value_list('DEP11::IconSizes'),
                        iconf)

        for pkgname, pkg in pkglist.items():
            for arch, data in pkg.items():
                package_fname = os.path.join (path, data['filename'])
                if not os.path.exists(package_fname):
                    print('Package not found: %s' % (package_fname))
                    continue
                pool.apply_async(extract_metadata,
                            (mde, suite.suite_name, pkgname, data['files'], data['binid'], package_fname, arch), callback=parse_results)
        pool.close()
        pool.join()

        # save new metadata to the database
        dpool.export(session)
        make_icon_tar(suite.suite_name, component)

        logger.log(["Completed metadata extraction for suite %s/%s" % (suite.suite_name, component)])

def write_component_files(session, suite, logger):
    '''
    Writes the metadata into Component-<arch>.yml.xz
    Ignores if ignore is True in the db
    '''

    # SQL to fetch metadata
    sql = """
        select distinct bd.metadata
        from
        bin_dep11 bd, binaries b, bin_associations ba,
        override o
        where bd.ignore = FALSE and bd.binary_id = b.id and b.package = o.package
        and o.component = :component_id and b.id = ba.bin
        and ba.suite = :suite_id and b.architecture = :arch_id
        """

    logger.log(["Writing DEP-11 files for %s" % (suite.suite_name)])
    for c in suite.components:
        # writing per <arch>
        for arch in suite.architectures:
            if arch.arch_string == "source":
                continue

            head_string = get_dep11_header(suite.suite_name, c.component_name)

            values = {
                'archive'  : suite.archive.path,
                'suite_id' : suite.suite_id,
                'suite'    : suite.suite_name,
                'component_id' : c.component_id,
                'component'    : c.component_name,
                'arch_id' : arch.arch_id,
                'arch'    : arch.arch_string
            }

            writer = DEP11DataFileWriter(**values)
            ofile = writer.open()
            ofile.write(head_string)

            result = session.execute(sql, values)
            for doc in result:
                ofile.write(doc[0])
            writer.close()

def write_hints_files(session, suite, logger):
    '''
    Writes the DEP-11 hints file (with issues and hints to improve the metadata)
    into DEP11Hints-<component>_<arch>.yml.gz in Dir::MetaInfoHints.
    '''

    # SQL to fetch hints
    sql = """
        select distinct bd.hints
        from
        bin_dep11 bd, binaries b, bin_associations ba,
        override o
        where bd.binary_id = b.id and b.package = o.package
        and o.component = :component_id and b.id = ba.bin
        and ba.suite = :suite_id and b.architecture = :arch_id
        """

    logger.log(["Writing DEP-11 hints files for %s" % (suite.suite_name)])
    for c in suite.components:
        # writing per arch
        for arch in suite.architectures:
            if arch.arch_string == "source":
                continue

            head_string = get_dep11_header(suite.suite_name, c.component_name)

            values = {
                'archive'  : suite.archive.path,
                'suite_id' : suite.suite_id,
                'suite'    : suite.suite_name,
                'component_id' : c.component_id,
                'component'    : c.component_name,
                'arch_id' : arch.arch_id,
                'arch'    : arch.arch_string
            }

            writer = DEP11HintsFileWriter(Config()["Dir::MetaInfoHints"], **values)
            ofile = writer.open()
            ofile.write(head_string)

            result = session.execute(sql, values)
            for doc in result:
                ofile.write(doc[0])
            writer.close()

def expire_dep11_data_cache(session, suitename, logger):
    '''
    Clears stale cache items per suite.
    '''

    # list for metadata we want to keep
    keep = list()

    # select all the binids with a package-name
    # (select all package-name from binaries)
    sql = """select bd.binary_id,b.package
    from bin_dep11 bd, binaries b
    where b.id = bd.binary_id"""

    q = session.execute(sql)
    result = q.fetchall()
    for r in result:
        keep.append("%s-%s" % (r[1], r[0]))

    glob_tmpl = "%s/*/*" % (os.path.join(Config()["Dir::MetaInfo"], suitename))
    for fname in glob.glob(glob_tmpl):
        if not os.path.basename(fname) in keep:
            logger.log(["Expiring DEP-11 cache directory: %s" % (fname)])
            rmtree(fname)

def main():
    cnf = Config()

    Arguments = [('h',"help","DEP11::Options::Help"),
                 ('s',"suite","DEP11::Options::Suite", "HasArg"),
                 ('e',"expire","DEP11::Options::ExpireCache"),
                 ('h',"write-hints","DEP11::Options::WriteHints"),
                 ]
    for i in ["help", "suite", "ExpireCache"]:
        if not cnf.has_key("DEP11::Options::%s" % (i)):
            cnf["DEP11::Options::%s" % (i)] = ""

    arguments = apt_pkg.parse_commandline(cnf.Cnf, Arguments, sys.argv)
    Options = cnf.subtree("DEP11::Options")

    if Options["Help"]:
        usage()
        return

    suitename = Options["Suite"]
    if not suitename:
        print("You need to specify a suite!")
        sys.exit(1)

    # check if we have some important config options set
    if not cnf.has_key("Dir::MetaInfo"):
        print("You need to specify a metadata export directory (Dir::MetaInfo)")
        sys.exit(1)
    if not cnf.has_key("DEP11::Url"):
        print("You need to specify a metadata public web URL (DEP11::Url)")
        sys.exit(1)
    if not cnf.has_key("DEP11::IconSizes"):
        print("You need to specify a list of allowed icon-sizes (DEP11::IconSizes)")
        sys.exit(1)
    if Options["WriteHints"] and not cnf.has_key("Dir::MetaInfoHints"):
        print("You need to specify an export directory for DEP-11 hints files (Dir::MetaInfoHints)")
        sys.exit(1)

    logger = daklog.Logger('generate-metadata')

    from daklib.dbconn import Component, DBConn, get_suite, Suite
    session = DBConn().session()
    suite = get_suite(suitename.lower(), session)

    if Options["ExpireCache"]:
        expire_dep11_data_cache(session, suitename, logger)

    process_suite(session, suite, logger)
    # export database content as Components-<arch>.xz YAML documents
    write_component_files(session, suite, logger)

    if Options["WriteHints"]:
        write_hints_files(session, suite, logger)

    # we're done
    logger.close()

if __name__ == "__main__":
    main()
