#!/usr/bin/env python

"""
Takes a .deb file as an argument and reads the metadata from
diffrent sources such as the xml files in usr/share/appdata
and .desktop files in usr/share/application. Also created
screenshot cache and tarball of all the icons of packages
beloging to a given suite.
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

###########################################################################

# The script is part of project under Google Summer of Code '14
# Project: AppStream/DEP-11 for the Debian Archive
# Mentor: Matthias Klumpp

###########################################################################

import apt_pkg
import yaml
import re
import sys
import urllib
import glob
import sha
import tarfile
import shutil
import datetime
import os
import os.path
import lxml.etree as et
from apt_inst import DebFile
from PIL import Image
from subprocess import CalledProcessError
from find_metainfo import *

from daklib import daklog
from daklib.daksubprocess import call, check_call
from daklib.filewriter import DEP11DataFileWriter
from daklib.config import Config
from daklib.dbconn import *
from daklib.dakmultiprocessing import DakProcessPool, PROC_STATUS_SUCCESS, PROC_STATUS_SIGNALRAISED

###########################################################################
DEP11_VERSION = "0.6"
time_str = str(datetime.date.today())
dep11_header = {
    "File": "DEP-11",
    "Version": DEP11_VERSION
}
###########################################################################

# TODO: Convert to SQLAlchemy ORM
# TODO: Move to dbconn.py
class DEP11Metadata():

    def __init__(self, session):
        self._session = session

    def insertdata(self, binid, yamldoc,flag):
        d = {"bin_id": binid, "yaml_data": yamldoc, "ignore":flag}
        sql = """insert into bin_dep11(binary_id,metadata,ignore)
        VALUES (:bin_id, :yaml_data, :ignore)"""
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

# for python2.7 not required for python3
def enc_dec(val):
    '''
    Handles encoding decoding for localised values
    '''
    try:
        val = unicode(val, "UTF-8")
    except TypeError:
        # already unicode
        pass
    try:
        val = str(val)
    except UnicodeEncodeError:
        pass
    return val


class DEP11YAMLDumper(yaml.Dumper):
    '''
    Custom YAML dumper, to ensure resulting YAML file can be read by
    all parsers (even the Java one)
    '''
    def increase_indent(self, flow=False, indentless=False):
        return super(DEP11YAMLDumper, self).increase_indent(flow, False)


class ProvidedItemType(object):
    '''
    Types supported as publicly provided interfaces. Used as keys in
    the 'Provides' field
    '''
    BINARY = 'binaries'
    LIBRARY = 'libraries'
    MIMETYPE = 'mimetypes'
    DBUS = 'dbus'
    PYTHON_2 = 'python2'
    PYTHON_3 = 'python3'
    FIRMWARE = 'firmware'
    CODEC = 'codecs'


class ComponentData:
    '''
    Used to store the properties of component data. Used by MetadataExtractor
    '''

    def __init__(self, suitename, component, binid, pkg):
        '''
        Used to set the properties to None.
        '''
        self._suitename = suitename
        self._component = component
        self._pkg = pkg
        self._binid = binid

        # properties
        self._compulsory_for_desktop = None
        self._ignore_reason = None
        self._ID = None
        self._type = None
        self._name = dict()
        self._categories = None
        self._icon = None
        self._summary = dict()
        self._description = None
        self._screenshots = None
        self._keywords = None
        self._archs = None
        self._provides = dict()
        self._url = None
        self._project_license = None
        self._project_group = None

    @property
    def ignore_reason(self):
        return self._ignore_reason

    @ignore_reason.setter
    def ignore_reason(self, val):
        self._ignore_reason = val

    @property
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, val):
        self._ID = val

    @property
    def kind(self):
        return self._type

    @kind.setter
    def kind(self, val):
        self._type = val

    @property
    def compulsory_for_desktop(self):
        return self._compulsory_for_desktop

    @compulsory_for_desktop.setter
    def compulsory_for_desktop(self, val):
        self._compulsory_for_desktop = val

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        self._name = val

    @property
    def categories(self):
        return self._categories

    @categories.setter
    def categories(self, val):
        self._categories = val

    @property
    def icon(self):
        return self._icon

    @icon.setter
    def icon(self, val):
        self._icon = val

    @property
    def summary(self):
        return self._summary

    @summary.setter
    def summary(self, val):
        self._summary = val

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, val):
        self._description = val

    @property
    def screenshots(self):
        return self._screenshots

    @screenshots.setter
    def screenshots(self, val):
        self._screenshots = val

    @property
    def keywords(self):
        return self._keywords

    @keywords.setter
    def keywords(self, val):
        self._keywords = val

    @property
    def archs(self):
        return self._archs

    @archs.setter
    def archs(self, val):
        self._archs = val

    @property
    def provides(self):
        return self._provides

    @provides.setter
    def provides(self, val):
        self._provides = val

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, val):
        self._url = val

    @property
    def project_license(self):
        return self._project_license

    @project_license.setter
    def project_license(self, val):
        self._project_license = val

    @property
    def project_group(self):
        return self._project_group

    @project_group.setter
    def project_group(self, val):
        self._project_group = val

    def add_provided_item(self, kind, value):
        if kind not in self.provides.keys():
            self.provides[kind] = list()
        self.provides[kind].append(value)

    def cleanup(self, dic):
        '''
        Remove cruft locale. And duplicates
        '''
        if dic.get('x-test'):
            dic.pop('x-test')
        if dic.get('xx'):
            dic.pop('xx')

        unlocalized = dic.get('C')
        if unlocalized:
            to_remove = []
            for k in dic.keys():
                if dic[k] == unlocalized and k != 'C':
                    dic.pop(k)

        return dic

    def serialize_to_dic(self):
        '''
        Return a dic with all the properties
        '''
        dic = {}
        dic['Packages'] = [self._pkg]
        if self.ID:
            dic['ID'] = self.ID
        if self.kind:
            dic['Type'] = self.kind

        # check if we need to print ignore information, instead
        # of exporting the software component
        if self.ignore_reason:
            dic['ID'] = self.ID
            dic['Ignored'] = True
            dic['Reason'] = self.ignore_reason
            return dic

        if self.name:
            dic['Name'] = self.cleanup(self.name)
        if self.summary:
            dic['Summary'] = self.cleanup(self.summary)
        if self.categories:
            dic['Categories'] = self.categories
        if self.description:
            dic['Description'] = self.description
        if self.keywords:
            dic['Keywords'] = self.keywords
        if self.screenshots:
            dic['Screenshots'] = self.screenshots
        if self.archs:
            dic['Architectures'] = self.archs
        if self.icon:
            dic['Icon'] = {'cached': self.icon}
        if self.url:
            dic['Url'] = self.url
        if self.provides:
            dic['Provides'] = self.provides
        if self.project_license:
            dic['ProjectLicense'] = self.project_license
        if self.project_group:
            dic['ProjectGroup'] = self.project_group
        if self.compulsory_for_desktop:
            dic['CompulsoryForDesktops'] = self.compulsory_for_desktop
        return dic


class MetadataExtractor:
    '''
    Takes a deb file and extracts component metadata from it.
    '''

    def __init__(self, suite_name, component, pkgname, metainfo_files, binid, pkg_fname):
        '''
        Initialize the object with List of files.
        '''
        self._filename = pkg_fname
        self._deb = None
        try:
            self._deb = DebFile(self._filename)
        except Exception as e:
            print ("Error reading deb file '%s': %s" % (self._filename , e))

        self._suite_name = suite_name
        self._component = component
        self._pkgname = pkgname
        self._mfiles = metainfo_files
        self._binid = binid

        component_basepath = "%s/%s/%s-%s" % (self._suite_name, self._component,
                                self._pkgname, str(self._binid))
        self._export_path = "%s/%s" % (Config()["Dir::MetaInfo"], component_basepath)
        self._public_url = "%s/%s" % (Config()["Url::DEP11"], component_basepath)

    def _deb_filelist(self):
        '''
        Returns a list of all files in a deb package
        '''
        files = list()
        if not self._deb:
            return files
        try:
            self._deb.data.go(lambda item, data: files.append(item.name))
        except SystemError:
            print ("ERROR: List of files for '%s' could not be read" % (self._filename))
            return None

        return files

    def _scale_screenshot(self, imgsrc, cpt_export_path, cpt_scr_url):
        '''
        scale images in three sets of two-dimensions
        (752x423 624x351 and 112x63)
        '''
        thumbnails = []
        name = os.path.basename(imgsrc)
        sizes = ['752x423', '624x351', '112x63']
        for size in sizes:
            wd, ht = size.split('x')
            img = Image.open(imgsrc)
            newimg = img.resize((int(wd), int(ht)), Image.ANTIALIAS)
            newpath = os.path.join(cpt_export_path, size)
            if not os.path.exists(newpath):
                os.makedirs(newpath)
            newimg.save(os.path.join(newpath, name))
            url = "%s/%s/%s" % (cpt_scr_url, size, name)
            thumbnails.append({'url': url, 'height': int(ht),
                               'width': int(wd)})

        return thumbnails

    def _fetch_screenshots(self, cpt):
        '''
        Fetches screenshots from the given url and
        stores it in png format.
        '''
        if not cpt.screenshots:
            # don't ignore metadata if screenshots itself is not present
            return True

        success = True
        shots = list()
        cnt = 1
        for shot in cpt.screenshots:
            # cache some locations which we need later
            origin_url = shot['source-image']['url']
            path = os.path.join(self._export_path, "screenshots")
            base_url = os.path.join(self._public_url, "screenshots")
            imgsrc = os.path.join(path, "source", "screenshot-%s.png" % (str(cnt)))
            try:
                image = urllib.urlopen(origin_url).read()
                if not os.path.exists(os.path.dirname(imgsrc)):
                    os.makedirs(os.path.dirname(imgsrc))
                f = open(imgsrc, 'wb')
                f.write(image)
                f.close()
            except Exception as e:
                print("Error while downloading screenshot from '%s' for component '%s': %s" % (origin_url, cpt.ID, str(e)))
                success = False
                continue

            try:
                img = Image.open(imgsrc)
                wd, ht = img.size
                shot['source-image']['width'] = wd
                shot['source-image']['height'] = ht
                shot['source-image']['url'] = os.path.join(base_url, "source", "screenshot-%s.png" % (str(cnt)))
                img.close()
            except Exception as e:
                print("Error while reading screenshot data for 'screenshot-%s.png' of component '%s': %s" % (str(cnt), cpt.ID, str(e)))
                success = False
                continue

            # scale_screenshots will return a list of
            # dicts with {height,width,url}
            shot['thumbnails'] = self._scale_screenshot(imgsrc, path, base_url)
            shots.append(shot)
            print("New screenshot cached from %s" % (origin_url))
            cnt = cnt + 1

        cpt.screenshots = shots
        return success

    def _store_icon(self, cpt, icon, filepath):
        '''
        Extracts the icon from the deb package and stores it in the cache.
        '''
        path = "%s/icons/" % (self._export_path)
        icon_name = "%s_%s" % (self._pkgname, os.path.basename(icon))
        cpt.icon = icon_name

        icon_store_location = "{0}/{1}".format(path, icon_name)
        if os.path.exists(icon_store_location):
            # we already extracted that icon, skip this step
            return True

        # filepath is checked because icon can reside in another binary
        # eg amarok's icon is in amarok-data
        if os.path.exists(filepath):
            try:
                icon_data = DebFile(filepath).data.extractdata(icon)
            except Exception as e:
                print("Error while extracting icon '%s': %s" % (filepath, e))
                return False

            if icon_data:
                if not os.path.exists(path):
                    os.makedirs(os.path.dirname(path))
                f = open(icon_store_location, "wb")
                f.write(icon_data)
                f.close()
                #! print("Saved icon %s." % (icon_name))
                return True
        return False

    def _fetch_icon(self, cpt, filelist):
        '''
        Searches for icon if absolute path to an icon
        is not given. Component with invalid icons are ignored
        '''
        if cpt.icon:
            icon = cpt.icon
            cpt.icon = os.path.basename (icon)

            # check if there is some kind of file-extension.
            # if there is none, the referenced icon is likely a stock icon, and we assume .png
            if not "." in cpt.icon:
                icon = icon + ".png"

            if not icon.endswith(('.png', '.svg', '.ico', '.xcf', '.gif', '.svgz')):
                cpt.ignore_reason = "Icon file '%s' uses an unsupported image file format." % (cpt.icon)
                return False

            if icon[1:] in filelist:
                return self._store_icon(cpt, icon[1:], self._filename)
            else:
                ext_allowed = ('.png', '.svg', '.ico', '.xcf', '.gif', '.svgz')
                for path in filelist:
                    if path.endswith(ext_allowed):
                        if 'pixmaps' in path or 'icons' in path:
                            return self._store_icon(cpt, path, self._filename)

                # the IconFinder runs it's own, new session, since we run multiprocess here
                ficon = IconFinder(self._pkgname, icon, self._binid)
                flist = ficon.query_icon()
                ficon.close()

                if flist:
                    filepath = (Config()["Dir::Pool"] +
                                cpt._component + '/' + flist[1])
                    return self._store_icon(cpt, flist[0], filepath)

                cpt.ignore_reason = "Icon '%s' was not found in the archive." % (cpt.icon)
                return False

        # keep metadata if Icon self itself is not present
        return True

    def _strip_comment(self, line=None):
        '''
        checks whether a line is a comment on .desktop file.
        '''
        line = line.strip()
        if line:
            if line[0] == "#":
                return None
            else:
                # when there's a comment inline
                if "#" in line:
                    line = line[0:line.find("#")]
                    return line
        return line

    def _read_desktop(self, dcontent, compdata):
        '''
        Parses a .desktop file and sets ComponentData properties
        '''
        lines = dcontent.splitlines()
        for line in lines:
            line = self._strip_comment(line)
            if line:
                # spliting into key-value pairs
                tray = line.split("=", 1)
                try:
                    key = tray[0].strip()
                    value = enc_dec(tray[1].strip())

                    if not value:
                        continue

                    # Should not specify encoding
                    if key.endswith('.UTF-8'):
                        key = key.strip('.UTF-8')

                    # Ignore the file if NoDisplay is true
                    if key == 'NoDisplay' and value == 'True':
                        # we ignore this .desktop file, shouldn't be displayed
                        break

                    if key == 'Type' and value != 'Application':
                        # ignore this file, isn't an application
                        break
                    else:
                        compdata.kind = 'desktop-app'

                    if key.startswith('Name') and value:
                        if key == 'Name':
                            compdata.name['C'] = value
                        else:
                            compdata.name[key[5:-1]] = value
                        continue

                    if key == 'Categories':
                        value = value.split(';')
                        value.pop()
                        compdata.categories = value
                        continue

                    if key.startswith('Comment') and value:
                        if key == 'Comment':
                            compdata.summary['C'] = value
                        else:
                            compdata.summary[key[8:-1]] = value
                        continue

                    if key.startswith('Keywords'):
                        value = re.split(';|,', value)
                        if not value[-1]:
                            value.pop()
                        if key[8:] == '':
                            if compdata.keywords:
                                if set(value) not in \
                                   [set(val) for val in
                                        compdata.keywords.values()]:
                                    compdata.keywords.update(
                                        {'C': map(enc_dec, value)}
                                    )
                            else:
                                compdata.keywords = {
                                    'C': map(enc_dec, value)
                                }
                        else:
                            if compdata.keywords:
                                if set(value) not in \
                                   [set(val) for val in
                                        compdata.keywords.values()]:
                                    compdata.keywords.update(
                                        {key[9:-1]: map(enc_dec, value)}
                                    )
                            else:
                                compdata.keywords = {
                                    key[9:-1]: map(enc_dec, value)
                                }
                        continue

                    if key == 'MimeType':
                        value = value.split(';')
                        if len(value) > 1:
                            value.pop()
                        for val in value:
                            compdata.add_provided_item(
                                ProvidedItemType.MIMETYPE, val
                            )
                        continue

                    if 'Architectures' in key:
                        val_list = value.split(',')
                        compdata.archs = val_list
                        continue

                    if key == 'Icon':
                        compdata.icon = value

                except:
                    pass

    def neat(self, s):
        '''
        Utility for parse_description_tag
        '''
        s = s.strip()
        s = " ".join(s.split())
        return s

    def _parse_description_tag(self, subs):
        '''
        Handles the description tag
        '''
        dic = {}
        for usubs in subs:
            attr_dic = usubs.attrib
            if attr_dic:
                for v in attr_dic.values():
                    key = v
            else:
                key = 'C'

            if usubs.tag == 'p':
                if dic.get(key):
                    dic[key] += "<p>%s</p>" % self.neat(enc_dec(usubs.text))
                else:
                    dic[key] = "<p>%s</p>" % self.neat(enc_dec(usubs.text))

            if usubs.tag == 'ul' or usubs.tag == 'ol':
                for k in dic.keys():
                    dic[k] += "<%s>" % usubs.tag

                for u_usubs in usubs:
                    attr_dic = u_usubs.attrib
                    if attr_dic:
                        for v in attr_dic.values():
                            key = v
                    else:
                        key = 'C'

                    if u_usubs.tag == 'li':
                        if dic.get(key):
                            dic[key] += "<li>%s</li>" % \
                                        self.neat(enc_dec(u_usubs.text))
                        else:
                            dic[key] = "<%s><li>%s</li>" % \
                                       (usubs.tag, self.neat(enc_dec
                                                             (u_usubs.text)))

                for k in dic.keys():
                    dic[k] += "</%s>" % usubs.tag
        return dic

    def _parse_screenshots_tag(self, subs):
        '''
        Handles screenshots.Caption source-image etc.
        '''
        shots = []
        for usubs in subs:
            # for one screeshot tag
            if usubs.tag == 'screenshot':
                screenshot = dict()
                attr_dic = usubs.attrib
                if attr_dic.get('type'):
                    if attr_dic['type'] == 'default':
                        screenshot['default'] = True
                # in case of old styled xmls
                url = usubs.text
                if url:
                    url = url.strip()
                    screenshot['source-image'] = {'url': url}
                    shots.append(screenshot)
                    continue

                # else look for captions and image tag
                for tags in usubs:
                    if tags.tag == 'caption':
                        # for localisation
                        attr_dic = tags.attrib
                        if attr_dic:
                            for v in attr_dic.values():
                                key = v
                        else:
                            key = 'C'

                        if screenshot.get('caption'):
                            screenshot['caption'][key] = enc_dec(tags.text)
                        else:
                            screenshot['caption'] = {key: enc_dec(tags.text)}
                    if tags.tag == 'image':
                        screenshot['source-image'] = {'url': tags.text}

                # only add the screenshot if we have a source image
                if screenshot.get ('source-image'):
                    shots.append(screenshot)

        return shots

    def _read_xml(self, xml_content, compdata):
        '''
        Reads the appdata from the xml file in usr/share/appdata.
        Sets ComponentData properties
        '''
        root = et.fromstring(xml_content)
        for key, val in root.attrib.iteritems():
            if key == 'type':
                if root.attrib['type'] == 'desktop':
                    compdata.kind = 'desktop-app'
                else:
                    # for other components like addon,codec, inputmethod etc
                    compdata.kind = root.attrib['type']

        for subs in root:
            if subs.tag == 'id':
                compdata.ID = subs.text

            if subs.tag == "description":
                desc = self._parse_description_tag(subs)
                compdata.description = desc

            if subs.tag == "screenshots":
                screen = self._parse_screenshots_tag(subs)
                compdata.screenshots = screen

            if subs.tag == "provides":
                for bins in subs:
                    if bins.tag == "binary":
                        compdata.add_provided_item(
                            ProvidedItemType.BINARY, bins.text
                        )
                    if bins.tag == 'library':
                        compdata.add_provided_item(
                            ProvidedItemType.LIBRARY, bins.text
                        )
                    if bins.tag == 'dbus':
                        compdata.add_provided_item(
                            ProvidedItemType.DBUS, bins.text
                        )
                    if bins.tag == 'firmware':
                        compdata.add_provided_item(
                            ProvidedItemType.FIRMWARE, bins.text
                        )
                    if bins.tag == 'python2':
                        compdata.add_provided_item(
                            ProvidedItemType.PYTHON_2, bins.text
                        )
                    if bins.tag == 'python3':
                        compdata.add_provided_item(
                            ProvidedItemType.PYTHON_3, bins.text
                        )
                    if bins.tag == 'codec':
                        compdata.add_provided_item(
                            ProvidedItemType.CODEC, bins.text
                        )

            if subs.tag == "url":
                if compdata.url:
                    compdata.url.update({subs.attrib['type']: subs.text})
                else:
                    compdata.url = {subs.attrib['type']: subs.text}

            if subs.tag == "project_license":
                compdata.project_license = subs.text

            if subs.tag == "project_group":
                compdata.project_group = subs.text

            if subs.tag == "CompulsoryForDesktop":
                if compdata.compulsory_for_desktop:
                    compdata.compulsory_for_desktop.append(subs.text)
                else:
                    compdata.compulsory_for_desktop = [subs.text]

    def get_cptdata(self):
        '''
        Reads the metadata from the xml file and the desktop files.
        And returns a list of ComponentData objects.
        '''
        if not self._deb:
            return list()
        suitename = self._suite_name
        filelist = self._deb_filelist()
        component_dict = dict()

        if not filelist:
            compdata = ComponentData(suitename, self._component, self._binid, self._pkgname)
            compdata.ignore_reason = "Could not determine file list for '%s'" % (os.path.basename(self._filename))
            return [compdata]

        component_dict = dict()
        # Reading xml files and associated .desktop
        for meta_file in self._mfiles:
            if meta_file.endswith(".xml"):
                xml_content = None
                compdata = ComponentData(suitename, self._component, self._binid, self._pkgname)

                try:
                    xml_content = str(self._deb.data.extractdata(meta_file))
                except Exception as e:
                    # inability to read an AppStream XML file is a valid ignore reason, skip this package.
                    compdata.ignore_reason = "Could not extract file '%s' from package '%s'. Error: %s" % (meta_file, self._filename, str(e))
                    return [compdata]
                if xml_content:
                    self._read_xml(xml_content, compdata)
                    # Reads the desktop files associated with the xml file
                    if compdata.ID:
                        component_dict[compdata.ID] = compdata
                    else:
                        # if there is no ID at all, we dump this component, since we cannot do anything with it at all
                        compdata.ignore_reason = "Could not determine an id for this component."
            else:
                # We have a .desktop file
                dcontent = None
                try:
                    dcontent = str(self._deb.data.extractdata(meta_file))
                except Exception as e:
                    print("Could not extract file '%s' from package '%s'. Error: %s" % (meta_file, self._filename, str(e)))
                    continue
                if not dcontent:
                    continue
                cpt_id = os.path.basename(meta_file)
                # in case we have a component with that ID already, extend it using the .desktop file data
                compdata = component_dict.get(cpt_id)
                if not compdata:
                    compdata = ComponentData(suitename, self._component, self._binid, self._pkgname)
                    compdata.ID = cpt_id
                self._read_desktop(dcontent, compdata)
                if not compdata.ignore_reason:
                    component_dict[cpt_id] = compdata

        for cpt in component_dict.values():
            self._fetch_icon(cpt, filelist)
            if cpt.kind == 'desktop-app' and not cpt.icon:
                if not cpt.ignore_reason:
                    cpt.ignore_reason = "GUI application, but no valid icon found."
            else:
                self._fetch_screenshots(cpt)

        return component_dict.values()

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
        makes a list of all the componentdata objects in a arch pool
        '''
        cpts = self._mcpts.get(arch)
        if not cpts:
            self._mcpts[arch] = dict()
            cpts = self._mcpts[arch]
        for c in compdatalist:
            if cpts.get(c.ID):
                print("WARNING: Duplicate ID detected: %s" % (c.ID))
                continue
            cpts[c.ID] = c

    def export(self, session):
        """
        Saves metadata in db (serialized to YAML)
        """
        for arch, cpts in self._mcpts.items():
            values = self._values
            values['architecture'] = arch
            dep11 = DEP11Metadata(session)
            for cdata in cpts.values():
                # get the metadata in YAML format
                metadata = yaml.dump(cdata.serialize_to_dic(), Dumper=DEP11YAMLDumper,
                            default_flow_style=False, explicit_start=True,
                            explicit_end=False, width=100, indent=2,
                            allow_unicode=True)
                # store metadata in database
                dep11.insertdata(cdata._binid, metadata, cdata.ignore_reason != None)
        # commit all changes
        session.commit()

##############################################################################


def make_icon_tar(suitename, component):
    '''
     icons-%(component)_%(size).tar.gz of each Component.
    '''

    icon_location_glob = os.path.join (Config()["Dir::MetaInfo"], suitename,  component, "*", "icons", "*.*")
    tar_location = os.path.join (Config()["Dir::Root"], "dists", suitename, component)

    icon_tar_fname = os.path.join(tar_location, "icons-%s_64px.tar.gz" % (component))
    tar = tarfile.open(icon_tar_fname, "w:gz")

    for filename in glob.glob(icon_location_glob):
        icon_name = os.path.basename (filename)
        tar.add(filename,arcname=icon_name)

    tar.close()

def extract_metadata(sn, c, pkgname, metainfo_files, binid, package_fname, arch):
    mde = MetadataExtractor(sn, c, pkgname, metainfo_files, binid, package_fname)
    cpt_list = mde.get_cptdata()

    data = dict()
    data['arch'] = arch
    data['cpts'] = cpt_list
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

def write_component_files(suite):
    '''
    Writes the metadata into Component-<arch>.xz
    Ignores if ignore is True in the db
    '''
    print("Writing DEP-11 files for %s" % (suite.suite_name))
    for component in [ c.component_name for c in suite.components ]:
        # writing per <arch>
        for arch in [ a.arch_string for a in suite.architectures ]:
            if arch == "source":
                continue

            head_dict = dep11_header
            head_dict['Origin'] = "%s-%s" % (suite.suite_name, component)
            head_string = yaml.dump(head_dict, Dumper=DEP11YAMLDumper,
                                    default_flow_style=False, explicit_start=True,
                                    explicit_end=False, width=200, indent=2)
            values = {
                'archive' : suite.archive.path,
                'suite' : suite.suite_name,
                'component' : component,
                'architecture' : arch
            }

            writer = DEP11DataFileWriter(**values)
            ofile = writer.open()
            ofile.write(head_string)
            dep11_data = BinDEP11Data(values)
            res = dep11_data.fetch_docs()
            for doc in res:
                ofile.write(doc[0])
            dep11_data.close()
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
    if not cnf.has_key("Url::DEP11"):
        print("You need to specify a metadata public web URL (Url::DEP11)")
        sys.exit(1)

    logger = daklog.Logger('generate-metadata')

    from daklib.dbconn import Component, DBConn, get_suite, Suite
    session = DBConn().session()
    suite = get_suite(suitename.lower(), session)

    if Options["ExpireCache"]:
        expire_dep11_data_cache(session, suitename)

    process_suite(session, suite, logger)
    # export database content as Components-<arch>.xz YAML documents
    write_component_files(suite)

    # we're done
    logger.close()

if __name__ == "__main__":
    main()
