#!/usr/bin/env python

"""
Checks binaries with a .desktop file or an AppStream upstream XML file.
Generates a dict with package name and associated appdata in
a list as value.
Finds icons for packages with missing icons.
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
    def __init__(self, package, icon, binid, suitename, component):
        self._session = DBConn().session()
        self._package = package
        self._icon = icon
        self._binid = binid
        self._suite_name = suitename
        self._component = component

        cnf = Config()
        self._icon_theme_packages = cnf.value_list('DEP11::IconThemePackages')
        self._icon_sizes = cnf.value_list('DEP11::IconSizes')

    def query_icon(self, size):
        '''
        function to query icon files from similar packages.
        Returns path of the icon
        '''
        ext_allowed = ('.png', '.svg', '.xcf', '.gif', '.svgz')

        if size:
            params = {
                'package': self._package + '%',
                'icon': 'usr/share/icons/hicolor/' + size + '/%' + self._icon + '%',
                'id': self._binid,
                'suitename': self._suite_name,
                'component': self._component,
            }
        else:
            params = {
                'package': self._package + '%',
                'icon': 'usr/share/pixmaps/' + self._icon + '%',
                'id': self._binid,
                'suitename': self._suite_name,
                'component': self._component
            }

        sql = """ select bc.file, f.filename
        from
        binaries b, bin_contents bc, files f,
        suite s, override o, component c
        where b.package like :package and b.file = f.id
        and (bc.file like :icon) and
        (bc.file not like '%.xpm' and bc.file not like '%.tiff')
        and b.id <> :id and b.id = bc.binary_id
        and  c.name = :component and c.id = o.component
        and o.package = b.package and s.suite_name = :suitename
        and s.id = o.suite"""

        result = self._session.execute(sql, params)
        rows = result.fetchall()

        if (size) and (not rows):
            for pkg in self._icon_theme_packages:
                # See if an icon-theme contains the icon.
                # Especially KDE software is packaged that way
                # FIXME: Make the hardcoded package-names a config option
                params = {
                    'package': pkg,
                    'icon': 'usr/share/icons/%/' + size + '/%' + self._icon + '%',
                    'id': self._binid,
                    'suitename': self._suite_name,
                    'component': self._component
                }
                result = self._session.execute(sql, params)
                rows = result.fetchall()
                if rows:
                    break

        for r in rows:
            path = str(r[0])
            filename = str(r[1])
            if path.endswith(self._icon) \
                or path.endswith(self._icon+'.png') \
                or path.endswith(self._icon+'.svg') \
                or path.endswith(self._icon+'.xcf') \
                or path.endswith(self._icon+'.gif') \
                or path.endswith(self._icon+'.svgz'):
                    return [path, filename]

        return False

    def get_icon(self):
        '''
        Returns the best possible icon available
        '''
        size_map_flist = dict()

        all_sizes = self._icon_sizes
        if not '256x256' in self._icon_sizes:
            all_sizes.append('256x256')

        for size in all_sizes:
            flist = self.query_icon(size)
            if (flist):
                size_map_flist[size] = flist

        # some software doesn't store icons in sized XDG directories.
        # catch these here, and assume that the size is 64x64
        if '64x64' not in size_map_flist.keys():
            flist = self.query_icon(None)
            if (flist):
                size_map_flist = {'64x64':flist}

        return size_map_flist

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
    # ##
    #clear_cached_dep11_data()
    '''
    #test
    f = IconFinder("amarok","amarok",140743,'aequorea','main')
    f.get_icon()
