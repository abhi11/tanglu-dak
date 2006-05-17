#!/usr/bin/env python

###########################################################
# generates partial package updates list

# idea and basic implementation by Anthony, some changes by Andreas
# parts are stolen from 'dak generate-releases'
#
# Copyright (C) 2004-6, 6  Anthony Towns <aj@azure.humbug.org.au>
# Copyright (C) 2004-5  Andreas Barth <aba@not.so.argh.org>

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


# < elmo> bah, don't bother me with annoying facts
# < elmo> I was on a roll


################################################################################

import sys, os, tempfile
import apt_pkg
import dak.lib.utils

################################################################################

projectB = None
Cnf = None
Logger = None
Options = None

################################################################################

def usage (exit_code=0):
    print """Usage: dak generate-index-diffs [OPTIONS] [suites]
Write out ed-style diffs to Packages/Source lists

  -h, --help            show this help and exit
  -c                    give the canonical path of the file
  -p                    name for the patch (defaults to current time)
  -n                    take no action
    """
    sys.exit(exit_code)


def tryunlink(file):
    try:
        os.unlink(file)
    except OSError:
        print "warning: removing of %s denied" % (file)

def smartstat(file):
    for ext in ["", ".gz", ".bz2"]:
        if os.path.isfile(file + ext):
            return (ext, os.stat(file + ext))
    return (None, None)

def smartlink(f, t):
    if os.path.isfile(f):
        os.link(f,t)
    elif os.path.isfile("%s.gz" % (f)):
        os.system("gzip -d < %s.gz > %s" % (f, t))
    elif os.path.isfile("%s.bz2" % (f)):
        os.system("bzip2 -d < %s.bz2 > %s" % (f, t))
    else:
        print "missing: %s" % (f)
        raise IOError, f

def smartopen(file):
    if os.path.isfile(file):
        f = open(file, "r")
    elif os.path.isfile("%s.gz" % file):
        f = create_temp_file(os.popen("zcat %s.gz" % file, "r"))
    elif os.path.isfile("%s.bz2" % file):
        f = create_temp_file(os.popen("bzcat %s.bz2" % file, "r"))
    else:
        f = None
    return f

def pipe_file(f, t):
    f.seek(0)
    while 1:
        l = f.read()
        if not l: break
        t.write(l)
    t.close()

class Updates:
    def __init__(self, readpath = None, max = 14):
        self.can_path = None
        self.history = {}
        self.history_order = []
        self.max = max
        self.readpath = readpath
        self.filesizesha1 = None

        if readpath:
          try:
            f = open(readpath + "/Index")
            x = f.readline()

            def read_hashs(ind, f, self, x=x):
                while 1:
                    x = f.readline()
                    if not x or x[0] != " ": break
                    l = x.split()
                    if not self.history.has_key(l[2]):
                        self.history[l[2]] = [None,None]
			self.history_order.append(l[2])
                    self.history[l[2]][ind] = (l[0], int(l[1]))
                return x

            while x:
                l = x.split()

                if len(l) == 0:
                    x = f.readline()
                    continue

                if l[0] == "SHA1-History:":
                    x = read_hashs(0,f,self)
                    continue

                if l[0] == "SHA1-Patches:":
                    x = read_hashs(1,f,self)
                    continue

                if l[0] == "Canonical-Name:" or l[0]=="Canonical-Path:":
                    self.can_path = l[1]

                if l[0] == "SHA1-Current:" and len(l) == 3:
                    self.filesizesha1 = (l[1], int(l[2]))

                x = f.readline()

          except IOError:
            0

    def dump(self, out=sys.stdout):
        if self.can_path:
            out.write("Canonical-Path: %s\n" % (self.can_path))
        
        if self.filesizesha1:
            out.write("SHA1-Current: %s %7d\n" % (self.filesizesha1))

        hs = self.history
        l = self.history_order[:]

        cnt = len(l)
        if cnt > self.max:
            for h in l[:cnt-self.max]:
                tryunlink("%s/%s.gz" % (self.readpath, h))
                del hs[h]
            l = l[cnt-self.max:]
	    self.history_order = l[:]

        out.write("SHA1-History:\n")
        for h in l:
            out.write(" %s %7d %s\n" % (hs[h][0][0], hs[h][0][1], h))
        out.write("SHA1-Patches:\n")
        for h in l:
            out.write(" %s %7d %s\n" % (hs[h][1][0], hs[h][1][1], h))

def create_temp_file(r):
    f = tempfile.TemporaryFile()
    while 1:
        x = r.readline()
        if not x: break
        f.write(x)
    r.close()
    del x,r
    f.flush()
    f.seek(0)
    return f

def sizesha1(f):
    size = os.fstat(f.fileno())[6]
    f.seek(0)
    sha1sum = apt_pkg.sha1sum(f)
    return (sha1sum, size)

def genchanges(Options, outdir, oldfile, origfile, maxdiffs = 14):
    if Options.has_key("NoAct"): 
        print "not doing anything"
        return

    patchname = Options["PatchName"]

    # origfile = /path/to/Packages
    # oldfile  = ./Packages
    # newfile  = ./Packages.tmp
    # difffile = outdir/patchname
    # index   => outdir/Index

    # (outdir, oldfile, origfile) = argv

    newfile = oldfile + ".new"
    difffile = "%s/%s" % (outdir, patchname)

    upd = Updates(outdir, int(maxdiffs))
    (oldext, oldstat) = smartstat(oldfile)
    (origext, origstat) = smartstat(origfile)
    if not origstat:
        print "%s doesn't exist" % (origfile)
        return
    if not oldstat:
        print "initial run"
        os.link(origfile + origext, oldfile + origext)
        return

    if oldstat[1:3] == origstat[1:3]:
        print "hardlink unbroken, assuming unchanged"
        return

    oldf = smartopen(oldfile)
    oldsizesha1 = sizesha1(oldf)

    # should probably early exit if either of these checks fail
    # alternatively (optionally?) could just trim the patch history

    if upd.filesizesha1:
        if upd.filesizesha1 != oldsizesha1:
            print "old file seems to have changed! %s %s => %s %s" % (upd.filesizesha1 + oldsizesha1)

    # XXX this should be usable now
    #
    #for d in upd.history.keys():
    #    df = smartopen("%s/%s" % (outdir,d))
    #    act_sha1size = sizesha1(df)
    #    df.close()
    #    exp_sha1size = upd.history[d][1]
    #    if act_sha1size != exp_sha1size:
    #        print "patch file %s seems to have changed! %s %s => %s %s" % \
    #            (d,) + exp_sha1size + act_sha1size

    if Options.has_key("CanonicalPath"): upd.can_path=Options["CanonicalPath"]

    if os.path.exists(newfile): os.unlink(newfile)
    smartlink(origfile, newfile)
    newf = open(newfile, "r")
    newsizesha1 = sizesha1(newf)
    newf.close()

    if newsizesha1 == oldsizesha1:
        os.unlink(newfile)
        oldf.close()
        print "file unchanged, not generating diff"
    else:
        if not os.path.isdir(outdir): os.mkdir(outdir)
        print "generating diff"
        w = os.popen("diff --ed - %s | gzip -c -9 > %s.gz" % 
                         (newfile, difffile), "w")
        pipe_file(oldf, w)
        oldf.close()

        difff = smartopen(difffile)
        difsizesha1 = sizesha1(difff)
        difff.close()

        upd.history[patchname] = (oldsizesha1, difsizesha1)
        upd.history_order.append(patchname)

        upd.filesizesha1 = newsizesha1

        os.unlink(oldfile + oldext)
        os.link(origfile + origext, oldfile + origext)
        os.unlink(newfile)

        f = open(outdir + "/Index", "w")
        upd.dump(f)
        f.close()


def main():
    global Cnf, Options, Logger

    os.umask(0002)

    Cnf = dak.lib.utils.get_conf()
    Arguments = [ ('h', "help", "Generate-Index-Diffs::Options::Help"),
                  ('c', None, "Generate-Index-Diffs::Options::CanonicalPath", "hasArg"),
                  ('p', "patchname", "Generate-Index-Diffs::Options::PatchName", "hasArg"),
                  ('r', "rootdir", "Generate-Index-Diffs::Options::RootDir", "hasArg"),
                  ('d', "tmpdir", "Generate-Index-Diffs::Options::TempDir", "hasArg"),
                  ('m', "maxdiffs", "Generate-Index-Diffs::Options::MaxDiffs", "hasArg"),
		  ('n', "n-act", "Generate-Index-Diffs::Options::NoAct"),
                ]
    suites = apt_pkg.ParseCommandLine(Cnf,Arguments,sys.argv)
    Options = Cnf.SubTree("Generate-Index-Diffs::Options")
    if Options.has_key("Help"): usage()

    maxdiffs = Options.get("MaxDiffs::Default", "14")
    maxpackages = Options.get("MaxDiffs::Packages", maxdiffs)
    maxcontents = Options.get("MaxDiffs::Contents", maxdiffs)
    maxsources = Options.get("MaxDiffs::Sources", maxdiffs)

    if not Options.has_key("PatchName"):
        format = "%Y-%m-%d-%H%M.%S"
        i,o = os.popen2("date +%s" % (format))
        i.close()
        Options["PatchName"] = o.readline()[:-1]
        o.close()

    AptCnf = apt_pkg.newConfiguration()
    apt_pkg.ReadConfigFileISC(AptCnf,dak.lib.utils.which_apt_conf_file())

    if Options.has_key("RootDir"): Cnf["Dir::Root"] = Options["RootDir"]

    if not suites:
        suites = Cnf.SubTree("Suite").List()

    for suite in suites:
        if suite == "Experimental": continue

        print "Processing: " + suite
        SuiteBlock = Cnf.SubTree("Suite::" + suite)

        if SuiteBlock.has_key("Untouchable"):
            print "Skipping: " + suite + " (untouchable)"
            continue

        suite = suite.lower()

        architectures = SuiteBlock.ValueList("Architectures")

        if SuiteBlock.has_key("Components"):
            components = SuiteBlock.ValueList("Components")
        else:
            components = []

        suite_suffix = Cnf.Find("Dinstall::SuiteSuffix")
        if components and suite_suffix:
            longsuite = suite + "/" + suite_suffix
        else:
            longsuite = suite

        tree = SuiteBlock.get("Tree", "dists/%s" % (longsuite))

        if AptCnf.has_key("tree::%s" % (tree)):
            sections = AptCnf["tree::%s::Sections" % (tree)].split()
        elif AptCnf.has_key("bindirectory::%s" % (tree)):
            sections = AptCnf["bindirectory::%s::Sections" % (tree)].split()
        else:
            aptcnf_filename = os.path.basename(dak.lib.utils.which_apt_conf_file())
            print "ALERT: suite %s not in %s, nor untouchable!" % (suite, aptcnf_filename)
            continue

        for architecture in architectures:
            if architecture == "all":
                continue

            if architecture != "source":
                # Process Contents
                file = "%s/Contents-%s" % (Cnf["Dir::Root"] + tree,
                        architecture)
                storename = "%s/%s_contents_%s" % (Options["TempDir"], suite, architecture)
                print "running contents for %s %s : " % (suite, architecture),
                genchanges(Options, file + ".diff", storename, file, \
                  Cnf.get("Suite::%s::Generate-Index-Diffs::MaxDiffs::Contents" % (suite), maxcontents))

            # use sections instead of components since dak.conf
            # treats "foo/bar main" as suite "foo", suitesuffix "bar" and
            # component "bar/main". suck.

            for component in sections:
                if architecture == "source":
                    longarch = architecture
                    packages = "Sources"
                    maxsuite = maxsources
                else:
                    longarch = "binary-%s"% (architecture)
                    packages = "Packages"
                    maxsuite = maxpackages

                file = "%s/%s/%s/%s" % (Cnf["Dir::Root"] + tree,
                           component, longarch, packages)
                storename = "%s/%s_%s_%s" % (Options["TempDir"], suite, component, architecture)
                print "running for %s %s %s : " % (suite, component, architecture),
                genchanges(Options, file + ".diff", storename, file, \
                  Cnf.get("Suite::%s::Generate-Index-Diffs::MaxDiffs::%s" % (suite, packages), maxsuite))

################################################################################

if __name__ == '__main__':
    main()