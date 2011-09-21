"""
Build Volante and optionally uploads it to s3.
"""

import os
import os.path
import shutil
import sys
import time
import re

from util import *

args = sys.argv[1:]
upload               = test_for_flag(args, "-upload")

def usage():
  print("build-release.py [-upload]")
  sys.exit(1)

def copy_to_dst_dir(src_path, dst_dir):
  name_in_obj_rel = os.path.basename(src_path)
  dst_path = os.path.join(dst_dir, name_in_obj_rel)
  shutil.copy(src_path, dst_path)

def main():
  if len(args) != 0:
    usage()
  verify_started_in_right_directory()

  ver = extract_volante_version(os.path.join("csharp", "src", "AssemblyInfo.cs"))
  log("Version: '%s'" % ver)

  filename_base = "Volante-%s" % ver
  s3_dir = "software/volante/rel"

  if upload:
    log("Will upload to s3 at %s" % s3_dir)

  s3_prefix = "%s/%s" % (s3_dir, filename_base)
  s3_zip           = s3_prefix + ".zip"

  s3_files = [s3_zip]

  if upload:
    map(ensure_s3_doesnt_exist, s3_files)

  shutil.rmtree(os.path.join("csharp", "bin"), ignore_errors=True)
  shutil.rmtree(os.path.join("csharp", "obj"), ignore_errors=True)

  os.chdir("csharp")
  run_cmd_throw("devenv", "Volante.sln", "/Rebuild", "Release", "/Project", "Volante")

  shutil.copytree("doc", os.path.join("bin", "Release", "doc"))
  os.chdir(os.path.join("bin", "Release"))
  ensure_path_exists("Volante.dll")
  dllsize = os.path.getsize("Volante.dll")
  ensure_path_exists("Volante.pdb")
  os.remove("Volante.xml")
  os.remove(os.path.join("doc", "Volante.xml"))

  zipname = os.path.join("%s.zip" % filename_base)
  zip_file(zipname, "Volante.pdb")
  zip_file(zipname, "Volante.dll", append=True)
  zip_dir(zipname, "doc")

  if not upload:
    print("Created csharp/bin/Release/" + filename_base)

  zipsize = os.path.getsize(zipname)
  jstxt  = 'var volanteLatestVer = %s;\n' % ver
  jstxt  = 'var volanteBuiltOn = "%s";\n' % time.strftime("%Y-%m-%d")
  jstxt += 'var volanteLatestName = "%s";\n' % s3_zip.split("/")[-1]
  jstxt += 'var volanteLatestZip = "http://kjkpub.s3.amazonaws.com/%s";\n' % s3_zip
  jstxt += 'var volanteZipSize = %d;' % zipsize
  jstxt += 'var volanteDllSize = %d;' % dllsize
  s3UploadFilePublic(zipname, s3_zip)
  s3UploadDataPublic(jstxt, "software/volante/latestver.js")

if __name__ == "__main__":
  main()
