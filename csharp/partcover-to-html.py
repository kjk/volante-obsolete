import cgi
import os
import os.path
import shutil
import sys
from xml.dom.minidom import parse

def usage_and_exit():
    print("Usage: partcover-to-html.py PARTCOVER_FILE.XML HTML_OUT_DIR")
    sys.exit(1)

def append_point(line_info, line, pt):
    if not line_info.has_key(line):
        line_info[line] = [pt]
        return
    if pt in line_info[line]: return
    line_info[line].append(pt)
    
class File(object):
    def __init__(self, id, url):
        self.id = id
        self.url = url
        self.html_file_name = None
        self.line_info = {}

    def calc_line_info(self, types):
        li = self.line_info
        for type in types:
            for method in type.methods:
                for pt in method.pts:
                    if pt.fid != self.id: continue
                    if pt.sl == pt.el:
                        append_point(li, pt.sl, [pt.sc, pt.ec])
                        continue
                    l = pt.sl
                    li[l] = [[pt.sc, -1]]
                    l += 1
                    while l < pt.el:
                        li[l] = None # means the whole line
                        l += 1
                    append_point(li, l, [0, pt.ec])

        for lno in li.keys():
            if None is li[lno]: continue
            li[lno].sort(lambda x,y: cmp(x[0],y[0]))
            arr = li[lno]
            prev = arr[0]
            for el in arr[1:]:
                if el[0] < prev[1]:
                    print("%s:%d" % (self.url, lno))
                    print(str(arr))
                    print("found overlap: %s, %s" % (str(prev), str(el)))
                    sys.exit(1)
                prev = el

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

g_type_id = 0
def next_type_id():
    global g_type_id
    g_type_id += 1
    return g_type_id

class Type(object):
    def __init__(self, asmref, name, flags, methods):
        self.id = next_type_id()
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
            total += m.coverageTotal
            covered += m.coverageCovered
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
        self._calc_coverage()

    def full_name(self):
        parts = self.sig.split(" ", 1)
        if len(parts) == 2:
            return parts[0] + " " + self.name + parts[1].strip()
        return self.name

    def get_file_line(self):
        for pt in self.pts:
            if pt.fid != None:
                return (pt.fid, pt.sl)
        return (None, None)

    def _calc_coverage(self):
        covered = 0
        total = self.bodysize
        for pt in self.pts:
            covered += pt.len
        self.coverageTotal = total
        self.coverageCovered = covered
        if 0 == total:
            self.coverage = float(100)
        else:
            self.coverage = (float(covered) * 100.0) / float(total)

    def get_coverage(self):
        return self.coverage

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

def dump_assemblies(assemblies, types):
    for v in assemblies.values():
        v.set_types(types)
        print("%03d : %s %.2f%% (%d out of %d)" % (v.id, v.name, v.get_coverage(), v.coverageCovered, v.coverageTotal))
        dump_types(v.types)

def html_name_from_path(path):
    parts = path.split("\\")
    if "impl" in path:
        s = parts[-2] + "_" + parts[-1]
        s = s.replace(".", "_")
    else:
        s = parts[-1].replace(".", "_")
    s = s + ".html"
    return s.lower()

def is_empty_line(l):
    return 0 == len(l.strip())

def annotate_line(l, line_info):
    if None == line_info: return """<span class="c">%s</span>""" % l
    # TODO: annotate just parts indicated by line_info
    return """<span class="c">%s</span>""" % l

def csharp_to_html(file, pathout, file_name):
    fin = open(file.url, "r")
    fout = open(pathout, "w")
    fout.write("""
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<style type=text/css> 
pre,code {font-size:9pt; font:Consolas,Monaco,"Courier New","DejaVu Sans Mono","Bitstream Vera Sans Mono",monospace;}
.c { background-color: #96EBA6; }
</style>
</head>
<body>
""")

    fout.write("""<pre><a href="index.html">Home</a> : %s</pre>""" % file_name)
    lines = []
    lno = 1
    for l in fin:
        lines.append(l)
        lno += 1

    # write out line numbers
    fout.write("""
<table cellpadding="0" cellspacing="0">
<tbody>
  <tr>
  <td style="margin:0px; vertical-align:top">
  <pre>""")
    for n in range(len(lines)):
        tmp = n + 1
        fout.write("<span>%d</span>\n" % tmp)
    fout.write("""
  </pre>
  </td>
""")

    # content
    fout.write("""
  <td  style="margin:0px; padding-left:8px; vertical-align:top" width="100%">
  <pre>""")
    lineno = 1
    li = file.line_info
    for l in lines:
        if is_empty_line(l):
            fout.write("""<div class="line" id="l%d"><br></div>""" % lineno)
        else:
            l = cgi.escape(l)
            if li.has_key(lineno):
                # TODO: need to take escaping into account when annotating
                l = annotate_line(l, li[lineno])
                fout.write("""<div class="line" id="l%d">%s</div>""" % (lineno, l))
            else:
                fout.write("""<div class="line" id="l%d">%s</div>""" % (lineno, l))
        lineno += 1
    fout.write("""
  </pre>
  </td>
  </tr>
</tbody>
</table>
""")
    fout.write("</body>\n</html>")
    fout.close()
    fin.close()

# @files is 
def gen_html_for_files(files, html_out_dir):
    for f in sorted(files):
        html_name = html_name_from_path(f.url)
        f.html_file_name = html_name
        print("%s => %s" % (f.url, html_name))
        html_path = os.path.join(html_out_dir, html_name)
        src_path = f.url.split("nachodb")[-1][1:]
        print(src_path)
        csharp_to_html(f, html_path, src_path)

# @types is an array of Type objects
# @files is a dict mapping file id to File object
def gen_html_for_types(types, files, html_out_dir, html_file_name):
    html_path = os.path.join(html_out_dir, html_file_name)
    fo = open(html_path, "w")
    fo.write("""<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">

<style type=text/css> 
.typename { font-weight: bold; }
.perc { color: red; }
</style>

<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js"></script>

<script type="text/javascript">
function toggleVisibility(id) {
  var el = $('#'+id);
  var val = el.css("display");
  if (val == "none") {
    el.css("display", "block");
  } else {
    el.css("display", "none");
  }
  return false;
}

</script>

</head>
<body>
<h1>Code coverage report for NachoDB</h1>
""")
    for type in types:
        fo.write("""<span class="typename"><a href="#" onclick="return toggleVisibility('cls%d');">%s</a></span>
<span class="perc">%.2f%%</span>:<br/>""" % (type.id, type.name, type.get_coverage()))
        fo.write("""<span id="cls%d" style="display:none">""" % type.id)
        fo.write("<ul>\n")
        for m in type.methods:
            (fid, lineno) = m.get_file_line()
            if fid == None:
                fo.write("""<li>%s <span class="perc">%.2f%%</span></li>\n""" % (m.full_name(), m.get_coverage()))
            else:
                file = files[fid]
                # a hack heuristic to align at the beginning of the function
                if lineno > 2:
                    lineno -= 2
                fo.write("""<li><a href="%s#l%d">%s</a> <span class="perc">%.2f%%</span></li>\n""" % (file.html_file_name, lineno, m.full_name(), m.get_coverage()))
        fo.write("</ul>\n")
        fo.write("</span>")
    fo.write("</body></html>")
    fo.close()

def main():
    if len(sys.argv) != 3:
        usage_and_exit()
    partcover_file = sys.argv[1]
    html_out_dir = sys.argv[2]
    if not os.path.exists(partcover_file):
        print("File '%s' doesn't exists" % partcover_file)
        print("")
        usage_and_exit()
    if os.path.exists(html_out_dir):
        shutil.rmtree(html_out_dir)
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

    #for v in files.values():
    #    print("%03d : %s" % (v.id, v.url))

    #dump_assemblies(assemblies, types)

    for file in files.values():
        file.calc_line_info(types)
    gen_html_for_files(files.values(), html_out_dir)
    gen_html_for_types(types, files, html_out_dir, "index.html")

if __name__ == "__main__":
    main()