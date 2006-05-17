#!/usr/bin/env python

# Remove obsolete .changes files from proposed-updates
# Copyright (C) 2001, 2002, 2003, 2004, 2006  James Troup <james@nocrew.org>

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

################################################################################

import os, pg, re, sys
import dak.lib.utils, dak.lib.database
import apt_pkg

################################################################################

Cnf = None
projectB = None
Options = None
pu = {}

re_isdeb = re.compile (r"^(.+)_(.+?)_(.+?).u?deb$")

################################################################################

def usage (exit_code=0):
    print """Usage: dak clean-proposed-updates [OPTION] <CHANGES FILE | ADMIN FILE>[...]
Remove obsolete changes files from proposed-updates.

  -v, --verbose              be more verbose about what is being done
  -h, --help                 show this help and exit

Need either changes files or an admin.txt file with a '.joey' suffix."""
    sys.exit(exit_code)

################################################################################

def check_changes (filename):
    try:
        changes = dak.lib.utils.parse_changes(filename)
        files = dak.lib.utils.build_file_list(changes)
    except:
        dak.lib.utils.warn("Couldn't read changes file '%s'." % (filename))
        return
    num_files = len(files.keys())
    for file in files.keys():
        if dak.lib.utils.re_isadeb.match(file):
            m = re_isdeb.match(file)
            pkg = m.group(1)
            version = m.group(2)
            arch = m.group(3)
            if Options["debug"]:
                print "BINARY: %s ==> %s_%s_%s" % (file, pkg, version, arch)
        else:
            m = dak.lib.utils.re_issource.match(file)
            if m:
                pkg = m.group(1)
                version = m.group(2)
                type = m.group(3)
                if type != "dsc":
                    del files[file]
                    num_files -= 1
                    continue
                arch = "source"
                if Options["debug"]:
                    print "SOURCE: %s ==> %s_%s_%s" % (file, pkg, version, arch)
            else:
                dak.lib.utils.fubar("unknown type, fix me")
        if not pu.has_key(pkg):
            # FIXME
            dak.lib.utils.warn("%s doesn't seem to exist in p-u?? (from %s [%s])" % (pkg, file, filename))
            continue
        if not pu[pkg].has_key(arch):
            # FIXME
            dak.lib.utils.warn("%s doesn't seem to exist for %s in p-u?? (from %s [%s])" % (pkg, arch, file, filename))
            continue
        pu_version = dak.lib.utils.re_no_epoch.sub('', pu[pkg][arch])
        if pu_version == version:
            if Options["verbose"]:
                print "%s: ok" % (file)
        else:
            if Options["verbose"]:
                print "%s: superseded, removing. [%s]" % (file, pu_version)
            del files[file]

    new_num_files = len(files.keys())
    if new_num_files == 0:
        print "%s: no files left, superseded by %s" % (filename, pu_version)
        dest = Cnf["Dir::Morgue"] + "/misc/"
        dak.lib.utils.move(filename, dest)
    elif new_num_files < num_files:
        print "%s: lost files, MWAAP." % (filename)
    else:
        if Options["verbose"]:
            print "%s: ok" % (filename)

################################################################################

def check_joey (filename):
    file = dak.lib.utils.open_file(filename)

    cwd = os.getcwd()
    os.chdir("%s/dists/proposed-updates" % (Cnf["Dir::Root"]))

    for line in file.readlines():
        line = line.rstrip()
        if line.find('install') != -1:
            split_line = line.split()
            if len(split_line) != 2:
                dak.lib.utils.fubar("Parse error (not exactly 2 elements): %s" % (line))
            install_type = split_line[0]
            if install_type not in [ "install", "install-u", "sync-install" ]:
                dak.lib.utils.fubar("Unknown install type ('%s') from: %s" % (install_type, line))
            changes_filename = split_line[1]
            if Options["debug"]:
                print "Processing %s..." % (changes_filename)
            check_changes(changes_filename)

    os.chdir(cwd)

################################################################################

def init_pu ():
    global pu

    q = projectB.query("""
SELECT b.package, b.version, a.arch_string
  FROM bin_associations ba, binaries b, suite su, architecture a
  WHERE b.id = ba.bin AND ba.suite = su.id
    AND su.suite_name = 'proposed-updates' AND a.id = b.architecture
UNION SELECT s.source, s.version, 'source'
  FROM src_associations sa, source s, suite su
  WHERE s.id = sa.source AND sa.suite = su.id
    AND su.suite_name = 'proposed-updates'
ORDER BY package, version, arch_string
""")
    ql = q.getresult()
    for i in ql:
        pkg = i[0]
        version = i[1]
        arch = i[2]
        if not pu.has_key(pkg):
            pu[pkg] = {}
        pu[pkg][arch] = version

def main ():
    global Cnf, projectB, Options

    Cnf = dak.lib.utils.get_conf()

    Arguments = [('d', "debug", "Clean-Proposed-Updates::Options::Debug"),
                 ('v',"verbose","Clean-Proposed-Updates::Options::Verbose"),
                 ('h',"help","Clean-Proposed-Updates::Options::Help")]
    for i in [ "debug", "verbose", "help" ]:
	if not Cnf.has_key("Clean-Proposed-Updates::Options::%s" % (i)):
	    Cnf["Clean-Proposed-Updates::Options::%s" % (i)] = ""

    arguments = apt_pkg.ParseCommandLine(Cnf,Arguments,sys.argv)
    Options = Cnf.SubTree("Clean-Proposed-Updates::Options")

    if Options["Help"]:
        usage(0)
    if not arguments:
        dak.lib.utils.fubar("need at least one package name as an argument.")

    projectB = pg.connect(Cnf["DB::Name"], Cnf["DB::Host"], int(Cnf["DB::Port"]))
    dak.lib.database.init(Cnf, projectB)

    init_pu()

    for file in arguments:
        if file.endswith(".changes"):
            check_changes(file)
        elif file.endswith(".joey"):
            check_joey(file)
        else:
            dak.lib.utils.fubar("Unrecognised file type: '%s'." % (file))

#######################################################################################

if __name__ == '__main__':
    main()
