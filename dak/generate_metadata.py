#!/usr/bin/env python

"""
Takes a .deb file as an argument and reads the metadata from
diffrent sources such as the xml files in usr/share/appdata
and .desktop files in usr/share/application. Also created
screenshot cache and tarball of all the icons of packages
beloging to a given suite.
"""

# Copyright (c) 2014 Abhishek Bhattacharjee <abhishek.bhattacharjee11@gmail.com>
# Copyright (c) 2014 Matthias Klumpp <mak@debian.org>
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

from dep11.find_metainfo import *
from dep11.extractor import MetadataExtractor
from dep11.component import DEP11Component, DEP11YamlDumper, get_dep11_header

from daklib import daklog
from daklib.daksubprocess import call, check_call
from daklib.filewriter import DEP11DataFileWriter
from daklib.config import Config
from daklib.dbconn import *
from daklib.dakmultiprocessing import DakProcessPool, PROC_STATUS_SUCCESS, PROC_STATUS_SIGNALRAISED

# TODO: Convert to SQLAlchemy ORM
# TODO: Move to dbconn.py
class DEP11Metadata():

    def __init__(self, session):
        self._session = session

    def insertdata(self, binid, yamldoc, hints, ignore):
        d = {"bin_id": binid, "yaml_data": yamldoc, "hints": hints, "ignore": ignore}
        sql = """insert into bin_dep11(binary_id,metadata,hints,ignore)
        VALUES (:bin_id, :yaml_data, :hints, :ignore)"""
        self._session.execute(sql, d)

    def removedata(self, suitename):
        sql = """delete from bin_dep11 where binary_id in
        (select distinct(b.id) from binaries b,override o,suite s
        where b.package = o.package and o.suite = s.id
        and s.suite_name= :suitename)"""
        self._session.execute(sql, {"suitename": suitename})
        self._session.commit()

def usage():
    print("""Usage: dak generate_metadata -s <suitename> [OPTION]
Extract DEP-11 metadata for the specified suite.

  -e, --expire-cache   Clear the icon/screenshot cache from stale data.
    """)

class MetadataPool:
    '''
    Keeps a pool of component metadata per arch per component
    '''

    def __init__(self, values):
        '''
        Sets the archname of the metadata pool.
        '''
        self._values = values
        self._mcpts = dict()

    def append_cptdata(self, arch, compdatalist):
        '''
        makes a list of all the DEP11Component objects in a arch pool
        '''
        cpts = self._mcpts.get(arch)
        if not cpts:
            self._mcpts[arch] = dict()
            cpts = self._mcpts[arch]
        for c in compdatalist:
            if cpts.get(c.cid):
                print("WARNING: Duplicate ID detected: %s" % (c.cid))
                c.add_ignore_reason("Adding this component would duplicate the ID '%s'." % (c.cid))
                c.cid = "~%s%s" % (str(uuid.uuid4()), c.cid)
            cpts[c.cid] = c

    def export(self, session):
        """
        Saves metadata in db (serialized to YAML)
        """
        for arch, cpts in self._mcpts.items():
            values = self._values
            values['architecture'] = arch
            dep11 = DEP11Metadata(session)
            for cpt in cpts.values():
                # get the metadata in YAML format
                metadata = cpt.to_yaml_doc()
                hints_str = ""
                hints = cpt.get_hints_dict()
                if hints:
                    hints_str = yaml.dump(hints, Dumper=DEP11YamlDumper,
                                default_flow_style=False, explicit_start=True,
                                explicit_end=False, width=100, indent=2,
                                allow_unicode=True)
                # store metadata in database
                dep11.insertdata(cpt._binid, metadata, hints_str, cpt.has_ignore_reason())
        # commit all changes
        session.commit()

##############################################################################


def make_icon_tar(suitename, component):
    '''
     icons-%(component)_%(size).tar.gz of each Component.
    '''
    sizes  = ['128x128', '64x64']
    for size in sizes:
        icon_location_glob = os.path.join (Config()["Dir::MetaInfo"], suitename,  component, "*", "icons", size, "*.*")
        tar_location = os.path.join (Config()["Dir::Root"], "dists", suitename, component)

        icon_tar_fname = os.path.join(tar_location, "icons-%s_%s.tar.gz" % (component, size))
        tar = tarfile.open(icon_tar_fname, "w:gz")

        for filename in glob.glob(icon_location_glob):
            icon_name = os.path.basename (filename)
            tar.add(filename,arcname=icon_name)

        tar.close()

def extract_metadata(sn, c, pkgname, metainfo_files, binid, package_fname, arch):
    mde = MetadataExtractor(sn, c, pkgname, metainfo_files, binid, package_fname)
    mde.process()

    data = dict()
    data['arch'] = arch
    data['cpts'] = mde.metadata
    data['message'] = "Processed package: %s (%s/%s)" % (pkgname, sn, arch)
    return (PROC_STATUS_SUCCESS, data)

def process_suite(session, suite, logger, force=False):
    '''
    Run by main to loop for different component and architecture.
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


        for pkgname, pkg in pkglist.items():
            for arch, data in pkg.items():
                package_fname = os.path.join (path, data['filename'])
                if not os.path.exists(package_fname):
                    print('Package not found: %s' % (package_fname))
                    continue
                pool.apply_async(extract_metadata,
                            (suite.suite_name, component, pkgname, data['files'], data['binid'], package_fname, arch), callback=parse_results)
        pool.close()
        pool.join()

        # Save metadata of all binaries of the Components-arch
        # This would require a lock
        dpool.export(session)
        make_icon_tar(suite.suite_name, component)

        logger.log(["Completed metadata extraction for suite %s/%s" % (suite.suite_name, component)])

def write_component_files(session, suite):
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

    print("Writing DEP-11 files for %s" % (suite.suite_name))
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

def expire_dep11_data_cache(session, suitename):
    '''
    Clears the stale cache items per suite.
    '''
    # dic that has pkg name as key and bin_ids as values in a list,
    # these are not to be deleted
    keep = list()
    dir_list = []
    print("Clearing stale cached data...")
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
            print("Removing DEP-11 cache directory: %s" % (fname))
            rmtree(fname)

    print("Cache pruned.")

def main():
    cnf = Config()

    Arguments = [('h',"help","DEP11::Options::Help"),
                 ('e',"expire","DEP11::Options::ExpireCache"),
                 ('s',"suite","DEP11::Options::Suite", "HasArg"),
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

    logger = daklog.Logger('generate-metadata')

    from daklib.dbconn import Component, DBConn, get_suite, Suite
    session = DBConn().session()
    suite = get_suite(suitename.lower(), session)

    if Options["ExpireCache"]:
        expire_dep11_data_cache(session, suitename)

    process_suite(session, suite, logger)
    # export database content as Components-<arch>.xz YAML documents
    write_component_files(session, suite)

    # we're done
    logger.close()

if __name__ == "__main__":
    main()
