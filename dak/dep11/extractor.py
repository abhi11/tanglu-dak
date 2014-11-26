#!/usr/bin/env python
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
import fnmatch
import urllib
from apt_inst import DebFile
import cStringIO as StringIO

import cairo
import rsvg
from tempfile import NamedTemporaryFile
from PIL import Image

from dep11.component import DEP11Component
from dep11.find_metainfo import IconFinder
from dep11.parsers import read_desktop_data, read_appstream_upstream_xml

from daklib.config import Config

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
        self._dep11_cpts = list()

        cnf = Config()
        component_basepath = "%s/%s/%s-%s" % (self._suite_name, self._component,
                                self._pkgname, str(self._binid))
        self._export_path = "%s/%s" % (cnf["Dir::MetaInfo"], component_basepath)
        self._public_url = "%s/%s" % (cnf["DEP11::Url"], component_basepath)

        self._icon_sizes = cnf.value_list('DEP11::IconSizes')

    @property
    def metadata(self):
        return self._dep11_cpts

    @metadata.setter
    def metadata(self, val):
        self._dep11_cpts = val

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
            if not origin_url:
                # url empty? skip this screenshot
                continue
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
                cpt.add_hint("Error while downloading screenshot from '%s' for component '%s': %s" % (origin_url, cpt.cid, str(e)))
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
                cpt.add_hint("Error while reading screenshot data for 'screenshot-%s.png' of component '%s': %s" % (str(cnt), cpt.cid, str(e)))
                success = False
                continue

            # scale_screenshots will return a list of
            # dicts with {height,width,url}
            shot['thumbnails'] = self._scale_screenshot(imgsrc, path, base_url)
            shots.append(shot)
            cnt = cnt + 1

        cpt.screenshots = shots
        return success

    def _icon_allowed(self, icon):
        ext_allowed = ('.png', '.svg', '.xcf', '.gif', '.svgz', '.jpg')
        if icon.endswith(ext_allowed):
            return True
        return False

    def _render_svg_to_png(self, data, store_path, width, height):
        '''
        Uses cairosvg to render svg data to png data.
        '''

        img =  cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
        ctx = cairo.Context(img)
        handler= rsvg.Handle(None, data)
        handler.render_cairo(ctx)

        img.write_to_png(store_path)

    def _store_icon(self, cpt, icon_path, deb_fname, size):
        '''
        Extracts the icon from the deb package and stores it in the cache.
        '''
        if not self._icon_allowed(icon_path):
            cpt.add_ignore_reason("Icon file '%s' uses an unsupported image file format." % (os.path.basename(icon_path)))
            return False

        if not os.path.exists(deb_fname):
            return False

        path = "%s/icons/%s/" % (self._export_path, size)
        icon_name = "%s_%s" % (self._pkgname, os.path.basename(icon_path))
        cpt.icon = icon_name

        icon_store_location = "{0}/{1}".format(path, icon_name)
        if os.path.exists(icon_store_location):
            # we already extracted that icon, skip this step
            return True

        # filepath is checked because icon can reside in another binary
        # eg amarok's icon is in amarok-data
        try:
            icon_data = DebFile(deb_fname).data.extractdata(icon_path)
        except Exception as e:
            print("Error while extracting icon '%s': %s" % (deb_fname, e))
            return False

        split = size.split('x', 2)
        icon_width = int(split[0])
        icon_height = int(split[1])

        if icon_data:
            if not os.path.exists(path):
                os.makedirs(os.path.dirname(path))

            if icon_name.endswith(".svg"):
                # render the SVG to a bitmap
                icon_store_location = icon_store_location.replace(".svg", ".png")
                self._render_svg_to_png(icon_data, icon_store_location, icon_width, icon_height)
                return True
            else:
                # we don't trust upstream to have the right icon size present, and therefore
                # always adjust the icon to the right size
                stream = StringIO.StringIO(icon_data)
                stream.seek(0)
                img = None
                try:
                    img = Image.open(stream)
                except Exception as e:
                    cpt.add_ignore_reason("Unable to open icon file '%s'. Error: %s" % (icon_name, str(e)))
                    return False
                newimg = img.resize((icon_width, icon_height), Image.ANTIALIAS)
                newimg.save(icon_store_location)
                return True

        return False

    def _fetch_icon(self, cpt, filelist):
        '''
        Searches for icon if absolute path to an icon
        is not given. Component with invalid icons are ignored
        '''
        if not cpt.icon:
            # keep metadata if Icon self itself is not present
            return True

        icon = cpt.icon
        cpt.icon = os.path.basename (icon)

        # list of large sizes to scal down, in order to find more icons
        large_sizes = ['256x256']

        success = False
        if icon.startswith("/"):
            if icon[1:] in filelist:
                return self._store_icon(cpt, icon[1:], self._filename, '64x64')
        else:
            ret = False
            # check if there is some kind of file-extension.
            # if there is none, the referenced icon is likely a stock icon, and we assume .png
            if "." in cpt.icon:
                icon_name = icon
            else:
                icon_name = icon + ".png"
            for size in self._icon_sizes:
                icon_path = "usr/share/icons/hicolor/%s/*/%s" % (size, icon_name)
                filtered = fnmatch.filter(filelist, icon_path)
                if filtered:
                    success = self._store_icon(cpt, filtered[0], self._filename, size) or success
            if not success:
                # we cheat and test for larger icons as well, which can be scaled down
                for size in large_sizes:
                    icon_path = "usr/share/icons/hicolor/%s/*/%s" % (size, icon_name)
                    filtered = fnmatch.filter(filelist, icon_path)
                    if filtered:
                       for asize in self._icon_sizes:
                            success = self._store_icon(cpt, filtered[0], self._filename, asize) or success

        if not success:
            last_pixmap = None
            # handle stuff in the pixmaps directory
            for path in filelist:
                if path.startswith("usr/share/pixmaps"):
                    icon_basename = os.path.basename(path)
                    if ((icon_basename == icon) or (os.path.splitext(icon_basename)[0] == icon)):
                        # the pixmap dir can contain icons in multiple formats, and store_icon() fails in case
                        # the icon format is not allowed. We therefore only exit here, if the icon has a valid format
                        if self._icon_allowed(path):
                            return self._store_icon(cpt, path, self._filename, '64x64')
                        last_pixmap = path
            if last_pixmap:
                # we don't do a global icon search anymore, since we've found an (unsuitable) icon
                # already
                cpt.add_ignore_reason("Icon file '%s' uses an unsupported image file format." % (os.path.basename(last_pixmap)))
                return False

            # the IconFinder uses it's own, new session, since we run multiprocess here
            ficon = IconFinder(self._pkgname, icon, self._binid, self._suite_name, self._component)
            icon_dict = ficon.get_icon()
            ficon.close()
            success = False
            if icon_dict:
                for size in self._icon_sizes:
                    if not size in icon_dict:
                        continue
                    filepath = (Config()["Dir::Pool"] +
                                cpt._component + '/' + icon_dict[size][1])
                    success = self._store_icon(cpt, icon_dict[size][0], filepath, size) or success
                if not success:
                    for size in large_sizes:
                        if not size in icon_dict:
                            continue
                        filepath = (Config()["Dir::Pool"] +
                                    cpt._component + '/' + icon_dict[size][1])
                        for asize in self._icon_sizes:
                            success = self._store_icon(cpt, icon_dict[size][0], filepath, asize) or success
                return success

            cpt.add_ignore_reason("Icon '%s' was not found in the archive or is not available in a suitable size (at least 64x64)." % (cpt.icon))
            return False

        return True

    def process(self):
        '''
        Reads the metadata from the xml file and the desktop files.
        And returns a list of DEP11Component objects.
        '''
        if not self._deb:
            return list()
        suitename = self._suite_name
        filelist = self._deb_filelist()
        component_dict = dict()

        if not filelist:
            compdata = DEP11Component(suitename, self._component, self._binid, self._pkgname)
            compdata.add_ignore_reason("Could not determine file list for '%s'" % (os.path.basename(self._filename)))
            return [compdata]

        component_dict = dict()
        # First process all XML files
        for meta_file in self._mfiles:
            if meta_file.endswith(".xml"):
                xml_content = None
                compdata = DEP11Component(suitename, self._component, self._binid, self._pkgname)

                try:
                    xml_content = str(self._deb.data.extractdata(meta_file))
                except Exception as e:
                    # inability to read an AppStream XML file is a valid ignore reason, skip this package.
                    compdata.add_ignore_reason("Could not extract file '%s' from package '%s'. Error: %s" % (meta_file, self._filename, str(e)))
                    return [compdata]
                if xml_content:
                    read_appstream_upstream_xml(xml_content, compdata)
                    # Reads the desktop files associated with the xml file
                    if compdata.cid:
                        component_dict[compdata.cid] = compdata
                    else:
                        # if there is no ID at all, we dump this component, since we cannot do anything with it at all
                        compdata.add_ignore_reason("Could not determine an id for this component.")

        # then extend the XML information with data from other files, e.g. .desktop or .pc files
        for meta_file in self._mfiles:
            if meta_file.endswith(".desktop"):
                # We have a .desktop file
                dcontent = None
                cpt_id = os.path.basename(meta_file)
                # in case we have a component with that ID already, extend it using the .desktop file data
                compdata = component_dict.get(cpt_id)
                if not compdata:
                    compdata = DEP11Component(suitename, self._component, self._binid, self._pkgname)
                    compdata.cid = cpt_id
                    component_dict[cpt_id] = compdata
                elif compdata.has_ignore_reason():
                    # don't add .desktop file information if we already decided to ignore this
                    continue

                try:
                    dcontent = str(self._deb.data.extractdata(meta_file))
                except Exception as e:
                    compdata.add_ignore_reason("Could not extract file '%s' from package '%s'. Error: %s" % (cpt_id, os.path.basename(self._filename), str(e)))
                    continue
                if not dcontent:
                    compdata.add_ignore_reason("File '%s' from package '%s' appeared empty." % (cpt_id, os.path.basename(self._filename)))
                    continue
                ret = read_desktop_data(dcontent, compdata)
                if not ret and compdata.has_ignore_reason():
                    # this means that reading the .desktop file failed and we should
                    # silently ignore this issue (since the file was marked to be invisible on purpose)
                    del component_dict[cpt_id]

        for cpt in component_dict.values():
            self._fetch_icon(cpt, filelist)
            if cpt.kind == 'desktop-app' and not cpt.icon:
                cpt.add_ignore_reason("GUI application, but no valid icon found.")
            else:
                self._fetch_screenshots(cpt)

        self._dep11_cpts = component_dict.values()
