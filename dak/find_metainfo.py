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


class appdata:
    def __init__(self, connstr=None):
        '''
        Initialize the variables and create a session.
        '''
        if connstr:
            self._constr = connstr
            self._engine = create_engine(self._constr)
            Session = sessionmaker(bind=self._engine)
            self._session = Session()
        else:
            # using Config
            self._constr = ''
            self._engine = None
            self._session = DBConn().session()
        self._idlist = {}
        self._deskdic = {}
        self._xmldic = {}
        self._commdic = {}
        self.arch_deblist = {}
        self._pkglist = {}

    def create_session(self, connstr):
        '''
        Establishes a session if it is not initialized during __init__.
        '''
        if connstr:
            engine = create_engine(connstr)
            Session = sessionmaker(bind=engine)
            return Session()
        else:
            print("Connection string invalid!")
            return None

    def find_meta_files(self, component, suitename, session=None):
        '''
        Find binaries with a .desktop files and/or .xml files.
        '''
        if session:
            self._session = session

        params = {
            'component': component,
            'suitename': suitename
            }

        # SQL logic:
        # select all the binaries that have a .desktop and xml files
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
        and bc.binary_id = rd.id
        """

        # do not repeat
        '''
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
        '''

        result = self._session.query("file", "filename", "name", "id",
                                     "arch_string", "package")\
                              .from_statement(sql).params(params)

        # create a dict with packagename:[.desktop and/or .xml files]

        for r in result:
            key = '%s/%s' % (r[2], r[1])
            if self.arch_deblist.get(r[4]):
                if key not in self.arch_deblist[r[4]]:
                    self._idlist[key] = r[3]
                    self._pkglist[key] = r[5]
                    self.arch_deblist[r[4]].append(key)
            else:
                self.arch_deblist[r[4]] = [key]
                self._idlist[key] = r[3]
                self._pkglist[key] = r[5]

            if r[0].endswith('.desktop'):
                if self._deskdic.get(key):
                    self._deskdic[key].append(r[0])
                else:
                    self._deskdic[key] = [r[0]]

            if r[0].endswith('.xml'):
                if self._xmldic.get(key):
                    self._xmldic[key].append(r[0])
                else:
                    self._xmldic[key] = [r[0]]

    def find_desktop(self, component, suitename, session=None):
        '''
        Find binaries with a .desktop file. To be used when, just
        the .desktop files are required.
        '''
        if session:
            self._session = session

        params = {
            'component': component,
            'suitename': suitename
            }

        # SQL logic:
        # select all the binaries that have a .desktop file
        sql = """
        with
        req_data as
        ( select distinct on(b.package) f.filename, c.name,
        b.id, a.arch_string, b.package
        from
        binaries b, bin_associations ba, suite s, files f,
        override o, component c, architecture a
        where b.type = 'deb' and b.file = f.id and b.package = o.package
        and o.component = c.id and c.name = :component and b.id = ba.bin
        and ba.suite = s.id and s.suite_name = :suitename and
        b.architecture = a.id order by b.package, b.version desc)

        select bc.file,rd.filename,rd.name,rd.id,rd.arch_string,rd.package
        from bin_contents bc,req_data rd
        where  bc.file like
        'usr/share/applications/%.desktop' and bc.binary_id = rd.id
        """

        result = self._session.query("file", "filename", "name", "id",
                                     "arch_string", "package")\
                              .from_statement(sql).params(params)

        # create a dict with packagename:[.desktop files]
        for r in result:
            key = '%s/%s' % (r[2], r[1])
            if self.arch_deblist.get(r[4]):
                if key not in self.arch_deblist[r[4]]:
                    self._idlist[key] = r[3]
                    self._pkglist[key] = r[5]
                    self.arch_deblist[r[4]].append(key)
            else:
                self.arch_deblist[r[4]] = [key]
                self._idlist[key] = r[3]
                self._pkglist[key] = r[5]

            if self._deskdic.get(key):
                self._deskdic[key].append(r[0])
            else:
                self._deskdic[key] = [r[0]]

    def find_xml(self, component, suitename, session=None):
        '''
        Find binaries with a .xml file.To be used when, just
        the .xml files are
        '''
        if session:
            self._session = session

        params = {
            'component': component,
            'suitename': suitename
            }

        # SQL logic:
        # select all the binaries that have a .xml file
        sql = """
        with
        req_data as
        ( select distinct on(b.package) f.filename, c.name,
        b.id, a.arch_string, b.package
        from
        binaries b, bin_associations ba, suite s, files f,
        override o, component c, architecture a
        where b.type = 'deb' and b.file = f.id and b.package = o.package
        and o.component = c.id and c.name = :component and b.id = ba.bin
        and ba.suite = s.id and s.suite_name = :suitename and
        b.architecture = a.id order by b.package, b.version desc)

        select bc.file,rd.filename,rd.name,rd.id,rd.arch_string,rd.package
        from bin_contents bc,req_data rd
        where  bc.file like 'usr/share/appdata/%.xml' and bc.binary_id = rd.id
        """

        result = self._session.query("file", "filename", "name", "id",
                                     "arch_string", "package")\
                              .from_statement(sql).params(params)

        # create a dict with package:[.xml files]
        for r in result:
            key = '%s/%s' % (r[2], r[1])
            if self.arch_deblist.get(r[4]):
                if key not in self.arch_deblist[r[4]]:
                    self._idlist[key] = r[3]
                    self._pkglist[key] = str(r[5])
                    self.arch_deblist[str(r[4])].append(key)
            else:
                self.arch_deblist[str(r[4])] = [key]
                self._idlist[key] = str(r[3])
                self._pkglist[key] = str(r[5])

            if self._xmldic.get(key):
                self._xmldic[key].append(r[0])
            else:
                self._xmldic[key] = [r[0]]

    def find_essentials(self, component, suitename, session=None):
        '''
        Only fetches the necessary data for processing
        binary_id, filename, arch_string
        '''

        if session:
            self._session = session

        params = {
            'component': component,
            'suitename': suitename
            }

        # SQL logic:
        sql = """SELECT distinct on (b.package) f.filename, c.name, b.id,
        a.arch_string, b.package
        from binaries b, bin_associations ba, suite s, files f,override o,
        component c, architecture a
        where b.type = 'deb' and b.file = f.id and o.package = b.package
        and o.component = c.id and c.name = :component and b.id = ba.bin
        and b.architecture = a.id and ba.suite = s.id and
        s.suite_name = :suitename
        order by b.package, b.version desc
        """

        result = self._session.execute(sql, params)
        rows = result.fetchall()

        for r in rows:
            # Every r will have an unique key(component/filename)
            key = '%s/%s' % (r[2], r[1])
            # if arch is already present
            if self.arch_deblist.get(r[3]):
                self._idlist[key] = r[2]
                self._pkglist[key] = r[4]
                self.arch_deblist[r[3]].append(key)
            # New arch
            else:
                self._idlist[key] = r[2]
                self._pkglist[key] = r[4]
                self.arch_deblist[r[3]] = [key]

    def comb_appdata(self):
        '''
        Combines both dictionaries and creates a new list with
        all the metdata (i.e the .desktop as well as .xml files.
        '''
        # copy the .desktop files dict
        self._commdic = self._deskdic

        for key, li in self._xmldic.iteritems():
            # if package already in list just append the xml files
            try:
                self._commdic[key] += li
            # else create a new entry for the package
            except KeyError:
                self._commdic[key] = li

    def printfiles(self):
        '''
        Prints the commdic,
        '''
        for k, l in self._commdic.iteritems():
            print(k + ': ', l)

    def close(self):
        self._session.close()

###########################################################################


class findicon():
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

    def queryicon(self):
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





############################################################################


def clear_cached_dep11_data(session, suitename):
    '''
    Clears the stale cache per suite.
    '''
    # dic that has pkg name as key and bin_ids as values in a list,
    # these are not to be deleted
    do_not_clear_list = {}
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
        if do_not_clear_list.get(r[1]):
            do_not_clear_list[r[1]].append(str(r[0]))
        else:
            do_not_clear_list[r[1]] = [str(r[0])]

    for pkg in do_not_clear_list.iterkeys():
        for i in glob.glob("%s%s/*/%s*/" % (Config()["Dir::MetaInfo"],
                                            suitename, pkg)):
            true = [True if "-"+binid not in i else False
                    for binid in do_not_clear_list[pkg]]

            # delete this directiory as the pkg-binid does not exist
            if all(true):
                dir_list.append(i)

    # remove the directories that are no longer required
    # (removes screenshots and icons)
    for d in dir_list:
        if os.path.exists(d):
            print("Removing DEP-11 cache directory: %s" % (d))
            rmtree(d)

    print("Cache pruned.")


#############################################################################

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
