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
from dep11.component import IconSize
from dep11.extractor import AbstractIconFinder

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


class IconFinder(AbstractIconFinder):
    '''
    To be used when icon is not found through regular method.This class
    searches icons of similar packages. Ignores the package with binid.
    '''
    def __init__(self, suitename, component):
        self._suite_name = suitename
        self._component = component

        cnf = Config()
        self._icon_theme_packages = cnf.value_list('DEP11::IconThemePackages')
        self._pool_dir = cnf["Dir::Pool"]

        self._allowed_exts = (".png")

    def query_icon(self, size, package, icon, binid):
        '''
        function to query icon files from similar packages.
        Returns path of the icon
        '''

        # we need our own session, since we use multiprocessing and an icon can be queried
        # at any time, and even in parallel
        session = DBConn().session()

        if size:
            params = {
                'package': package + '%',
                'icon': 'usr/share/icons/hicolor/' + size + '/%' + icon + '%',
                'id': binid,
                'suitename': self._suite_name,
                'component': self._component,
            }
        else:
            params = {
                'package': package + '%',
                'icon': 'usr/share/pixmaps/' + icon + '%',
                'id': binid,
                'suitename': self._suite_name,
                'component': self._component
            }

        sql = """ select bc.file, f.filename
        from
        binaries b, bin_contents bc, files f,
        suite s, override o, component c, bin_associations ba
        where b.package like :package and b.file = f.id
        and (bc.file like :icon) and
        (bc.file not like '%.xpm' and bc.file not like '%.tiff')
        and b.id <> :id and b.id = bc.binary_id
        and  c.name = :component and c.id = o.component
        and o.package = b.package and b.id = ba.bin
        and ba.suite = s.id and s.suite_name = :suitename"""

        result = session.execute(sql, params)
        rows = result.fetchall()

        if (size) and (size != "scalable") and (not rows):
            for pkg in self._icon_theme_packages:
                # See if an icon-theme contains the icon.
                # Especially KDE software is packaged that way
                # FIXME: Make the hardcoded package-names a config option
                params = {
                    'package': pkg,
                    'icon': 'usr/share/icons/%/' + size + '/%' + icon + '%',
                    'id': binid,
                    'suitename': self._suite_name,
                    'component': self._component
                }
                result = session.execute(sql, params)
                rows = result.fetchall()
                if rows:
                    break

        # we don't need the session anymore beyond this point
        session.close()

        for r in rows:
            path = str(r[0])
            deb_fname = os.path.join(self._pool_dir, self._component, str(r[1]))
            if path.endswith(icon):
                return {'icon_fname': path, 'deb_fname': deb_fname}
            for ext in self._allowed_exts:
                if path.endswith(icon+ext):
                    return {'icon_fname': path, 'deb_fname': deb_fname}

        return False

    def get_icons(self, package, icon, sizes, binid):
        '''
        Returns the best possible icon available
        '''
        size_map_flist = dict()

        for size in sizes:
            flist = self.query_icon(str(size), package, icon, binid)
            if (flist):
                size_map_flist[size] = flist

        if '64x64' not in size_map_flist:
            # see if we can find a scalable vector graphic as icon
            # we assume "64x64" as size here, and resize the vector
            # graphic later.
            flist = self.query_icon("scalable", package, icon, binid)
            if (flist):
                size_map_flist = {'64x64': flist}
            else:
                # some software doesn't store icons in sized XDG directories.
                # catch these here, and assume that the size is 64x64
                flist = self.query_icon(None, package, icon, binid)
                if (flist):
                    size_map_flist = {'64x64': flist}

        return size_map_flist

    def set_allowed_icon_extensions(self, exts):
        self._allowed_exts = exts
