import os
import os.path
import sys
from xml.dom.minidom import parse

def usage_and_exit():
    print("Usage: partcover-to-html.py PARTCOVER_FILE.XML HTML_OUT_DIR")
    sys.exit(1)

class File(object):
    def __init__(self, id, url):
        self.id = id
        self.url = url

class Assembly(object):
    def __init__(self, id, name, module, domain, domainIdx):
        self.id = id
        self.name = name
        self.module = module
        self.domain = domain
        self.domainIdx = domainIdx
        self.types = []
        self.coverage = None
        self.coverageTotal = None
        self.coverageCovered = None

    def set_types(self, types):
        for t in types:
            if self.id == t.asmref:
                self.types.append(t)
        self._calc_coverage()

    def _calc_coverage(self):
        total = 0
        covered = 0
        for t in self.types:
            total += t.coverageTotal
            covered += t.coverageCovered
        self.coverageTotal = total
        self.coverageCovered = covered
        if 0 == total:
            self.coverage = float(100)
        else:
            self.coverage = (float(covered) * 100.0) / float(total)

    def get_coverage(self):
        if self.coverage == None:
            self._calc_coverage(types)
        return self.coverage

class Type(object):
    def __init__(self, asmref, name, flags, methods):
        self.asmref = asmref
        self.name = name
        self.flags = flags
        methods.sort(lambda x,y: cmp(x.name.lower(), y.name.lower()))
        self.methods = methods
        self._calc_coverage()

    def _calc_coverage(self):
        total = 0
        covered = 0
        for m in self.methods:
            total += m.bodysize
            for pt in m.pts:
                covered += pt.len
        self.coverageTotal = total
        self.coverageCovered = covered
        if 0 == total:
            self.coverage = float(100)
        else:
            self.coverage = (float(covered) * 100.0) / float(total)

    def get_coverage(self):
        return self.coverage

class Method(object):
    def __init__(self, name, sig, bodysize, flags, iflags, pts):
        self.name = name
        self.sig = sig
        self.bodysize = bodysize
        self.flags = flags
        self.iflags = iflags
        self.pts = pts

class Pt(object):
    def __init__(self, visit, pos, len, fid, sl, sc, el, ec):
        self.visit = visit
        self.pos = pos
        self.len = len
        self.fid = fid
        self.sl = sl
        self.sc = sc
        self.el = el
        self.ec = ec

def extractFile(el):
    attrs = el.attributes
    id = int(attrs["id"].value)
    url = attrs["url"].value
    return File(id, url)

def extractAssembly(el):
    attrs = el.attributes
    id = int(attrs["id"].value)
    name = attrs["name"].value
    module = attrs["module"].value
    domain = attrs["domain"].value
    domainIdx = int(attrs["domainIdx"].value)
    return Assembly(id, name, module, domain, domainIdx)

def extractType(el):
    attrs = el.attributes
    asmref = int(attrs["asmref"].value)
    name = attrs["name"].value
    flags = int(attrs["flags"].value)
    methods = []
    for mel in el.getElementsByTagName("Method"):
        v = extractMethod(mel)
        methods.append(v)
    return Type(asmref, name, flags, methods)

def extractMethod(el):
    attrs = el.attributes
    name = attrs["name"].value
    sig = attrs["sig"].value
    bodysize = int(attrs["bodysize"].value)
    flags = int(attrs["flags"].value)
    iflags = int(attrs["iflags"].value)
    pts = []
    for pel in el.getElementsByTagName("pt"):
        v = extractPt(pel)
        pts.append(v)
    return Method(name, sig, bodysize, flags, iflags, pts)

def extractPt(pel):
    attrs = pel.attributes
    visit = int(attrs["visit"].value)
    pos = int(attrs["pos"].value)
    len = int(attrs["len"].value)
    fid, sl, sc, el, ec = [None]*5
    if pel.hasAttribute("fid"):
        fid = int(attrs["fid"].value)
        sl = int(attrs["sl"].value)
        sc = int(attrs["sc"].value)
        el = int(attrs["el"].value)
        ec = int(attrs["ec"].value)
    return Pt(visit, pos, len, fid, sl, sc, el, ec)

def dump_types(types):
    for t in types:
        print("%s %.2f%% (%d out of %d)" % (t.name, t.get_coverage(), t.coverageCovered, t.coverageTotal))
        for m in t.methods:
            print("  %s (%d)" % (m.name,m.bodysize))

def main():
    if len(sys.argv) != 3:
        usage_and_exit()
    partcover_file = sys.argv[1]
    html_out_dir = sys.argv[2]
    if not os.path.exists(partcover_file):
        print("File '%s' doesn't exists" % partcover_file)
        print("")
        usage_and_exit()
    if not os.path.exists(html_out_dir):
        os.makedirs(html_out_dir)
    dom = parse(partcover_file)

    files = {}
    for el in dom.getElementsByTagName("File"):
        v = extractFile(el)
        files[v.id] = v

    assemblies = {}
    for el in dom.getElementsByTagName("Assembly"):
        v = extractAssembly(el)
        assemblies[v.id] = v

    types = []
    for el in dom.getElementsByTagName("Type"):
        types.append(extractType(el))
    types.sort(lambda x,y: cmp(x.name.lower(), y.name.lower()))

    for v in files.values():
        print("%03d : %s" % (v.id, v.url))

    for v in assemblies.values():
        v.set_types(types)
        print("%03d : %s %.2f%% (%d out of %d)" % (v.id, v.name, v.get_coverage(), v.coverageCovered, v.coverageTotal))
        dump_types(v.types)


if __name__ == "__main__":
    main()
