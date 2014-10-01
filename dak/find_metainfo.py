#!/usr/bin/env python

"""
Checks binaries with a .desktop file or an AppStream upstream XML file.
Generates a dict with package name and associated appdata in
a list as value.
Finds icons for packages with missing icons.
"""

# Copyright (C) 2014 Abhishek Bhattacharjee <abhishek.bhattacharjee11@gmail.com>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

############################################################################

# the script is part of project under Google Summer of Code '14
# Project: AppStream/DEP-11 for the Debian Archive
# Mentor: Matthias Klumpp

############################################################################


import os
import glob
from shutil import rmtree
from daklib.dbconn import *
from daklib.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class MetaInfoFinder:
    def __init__(self, session):
        '''
        Initialize the variables and create a session.
        '''

        self._session = session

    def find_meta_files(self, component, suitename):
        '''
        Find binaries with a .desktop files and/or .xml files.
        '''

        params = {
            'component': component,
            'suitename': suitename
            }

        # SQL logic:
        # select all the binaries that have a .desktop and xml files
        # do not repeat processing of deb files that are already processed

        sql = """
        with
        req_data as
        ( select distinct on(b.package) f.filename, c.name, b.id,
        a.arch_string, b.package
        from
        binaries b, bin_associations ba, suite s, files f, override o,
        component c, architecture a
        where b.type = 'deb' and b.file = f.id and b.package = o.package
        and o.component = c.id and c.name = :component and b.id = ba.bin
        and ba.suite = s.id and s.suite_name = :suitename and
        b.architecture = a.id order by b.package, b.version desc)

        select bc.file,rd.filename,rd.name,rd.id,rd.arch_string,rd.package
        from bin_contents bc,req_data rd
        where (bc.file like 'usr/share/appdata/%.xml' or
        bc.file like 'usr/share/applications/%.desktop')
        and bc.binary_id = rd.id and rd.id not in
        (select binary_id from bin_dep11)
        """

        # FIXME: We get only one hit for one arch for some reason...
        result = self._session.query("file", "filename", "name", "id",
                                     "arch_string", "package")\
                              .from_statement(sql).params(params)

        # create a dict with packagename:[.desktop and/or .xml files]

        interesting_pkgs = dict()
        for r in result:
            fname = '%s/%s' % (r[2], r[1])
            pkg_name = r[5]
            arch_name = r[4]
            if not interesting_pkgs.get(pkg_name):
                interesting_pkgs[pkg_name] = dict()
            pkg = interesting_pkgs[pkg_name]
            if not pkg.get(arch_name):
                pkg[arch_name] = dict()

            pkg[arch_name]['filename'] = fname
            pkg[arch_name]['binid'] = r[3]
            if not pkg[arch_name].get('files'):
                pkg[arch_name]['files'] = list()
            ifiles = pkg[arch_name]['files']
            ifiles.append(r[0])

        return interesting_pkgs


###########################################################################


class IconFinder():
    '''
    To be used when icon is not found through regular method.This class
    searches icons of similar packages. Ignores the package with binid.
    '''
    def __init__(self, package, icon, binid):
        self._params = {
            'package': '%' + package + '%',
            'icon1': 'usr/share/icons/%' + icon + '%',
            'icon2': 'usr/share/pixmaps/%' + icon + '%',
            'id': binid
        }
        self._session = DBConn().session()
        self._icon = icon

    def query_icon(self):
        '''
        function to query icon files from similar packages.
        Returns path of the icon
        '''
        sql = """ select bc.file, f.filename
        from binaries b, bin_contents bc, files f
        where b.file = f.id and b.package like :package
        and (bc.file like :icon1 or bc.file like :icon2) and
        (bc.file not like '%.xpm' and bc.file not like '%.tiff')
        and b.id <> :id and b.id = bc.binary_id"""

        result = self._session.execute(sql, self._params)
        rows = result.fetchall()

        for r in rows:
            path = str(r[0])
            filename = str(r[1])
            if path.endswith(self._icon+'.png')\
               or path.endswith(self._icon+'.svg')\
               or path.endswith(self._icon+'.ico')\
               or path.endswith(self._icon+'.xcf')\
               or path.endswith(self._icon+'.gif')\
               or path.endswith(self._icon+'.svgz'):
                # Write the logic to sekect the best icon of all
                return [path, filename]

        return False

    def close(self):
        """
        Closes the session
        """
        self._session.close()


class BinDEP11Data():
    def __init__(self,params):
        self._params = params
        self._session = DBConn().session()

    def fetch_docs(self):
        '''
        Fetches the YAML docs if the ignore field is false
        Per arch per component per suite basis
        '''
        # SQL to fetch metadata
        sql = """
        select bd.metadata
        from
        bin_dep11 bd, binaries b, bin_associations ba, suite s,
        override o, component c, architecture a
        where bd.ignore = FALSE and bd.binary_id = b.id and b.package = o.package
        and o.component = c.id and c.name = :component and b.id = ba.bin
        and ba.suite = s.id and s.suite_name = :suite and
        b.architecture = a.id and a.arch_string = :architecture
        """

        result = self._session.execute(sql, self._params)
        rows = result.fetchall()
        return rows

    def close(self):
        """
        Closes the session
        """
        self._session.close()

# For testing

if __name__ == "__main__":
    '''   # ##
    ap = appdata()
    ap.comb_appdata()
    ap.printfiles()
    f = findicon("aqemu","aqemu",48)
    f.queryicon()
    # ##
    #clear_cached_dep11_data()
    '''
    ap = appdata()
    #    ap.find_desktop(component = 'main', suitename='aequorea')
    #    ap.find_xml(component = 'main', suitename='aequorea')
    ap.find_meta_files(component='main',suitename='bartholomea')
    for arc in ap.arch_deblist.keys():
        print arc
        for k in ap.arch_deblist[arc]:
            print (k,ap._pkglist[k],ap._idlist[k])

        print(ap._deskdic)
    ap.close()
