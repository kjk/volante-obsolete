"""
Build Volante and optionally uploads it to s3.
"""

import os
import os.path
import shutil
import sys
import time
import re

from util import log, run_cmd_throw, test_for_flag, s3UploadFilePublic
from util import s3UploadDataPublic, ensure_s3_doesnt_exist, ensure_path_exists
from util import zip_file, extract_volante_version, verify_started_in_right_directory

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
  s3_dir = "sumatrapdf/rel"

  if upload:
    log("Will upload to s3 at %s" % s3_dir)

  s3_prefix = "%s/%s" % (s3_dir, filename_base)
  s3_zip           = s3_prefix + ".zip"

  s3_files = [s3_zip]

  if upload:
    map(ensure_s3_doesnt_exist, s3_files)

"""
  if not testing and not build_test_installer:
    shutil.rmtree("obj-rel", ignore_errors=True)

  obj_dir = "obj-rel"
  config = "CFG=rel"
  if build_test_installer and not build_prerelease:
    obj_dir = "obj-dbg"
    config = "CFG=dbg"
  extcflags = ""
  if build_prerelease:
    extcflags = "EXTCFLAGS=-DSVN_PRE_RELEASE_VER=%s" % ver

  run_cmd_throw("nmake", "-f", "makefile.msvc", config, extcflags, "all_sumatrapdf")
  build_installer_data(obj_dir)
  run_cmd_throw("nmake", "-f", "makefile.msvc", "Installer", config, extcflags)

  if build_test_installer:
    sys.exit(0)

  exe = os.path.join(obj_dir, "SumatraPDF.exe")
  installer = os.path.join(obj_dir, "Installer.exe")
  pdb_zip = os.path.join(obj_dir, "%s.pdb.zip" % filename_base)

  zip_file(pdb_zip, os.path.join(obj_dir, "libmupdf.pdb"))
  zip_file(pdb_zip, os.path.join(obj_dir, "SumatraPDF-no-MuPDF.pdb"), append=True)
  zip_file(pdb_zip, os.path.join(obj_dir, "SumatraPDF.pdb"), "%s.pdb" % filename_base, append=True)

  builds_dir = os.path.join("builds", ver)
  if os.path.exists(builds_dir):
    shutil.rmtree(builds_dir)
  os.makedirs(builds_dir)

  copy_to_dst_dir(exe, builds_dir)
  copy_to_dst_dir(installer, builds_dir)
  copy_to_dst_dir(pdb_zip, builds_dir)

  if not build_prerelease:
    exe_zip = os.path.join(obj_dir, "%s.zip" % filename_base)
    zip_file(exe_zip, exe, "SumatraPDF.exe", compress=True)
    ensure_path_exists(exe_zip)
    copy_to_dst_dir(exe_zip, builds_dir)

  if upload or upload_tmp:
    if build_prerelease:
      jstxt  = 'var sumLatestVer = %s;\n' % ver
      jstxt  = 'var sumBuiltOn = "%s";\n' % time.strftime("%Y-%m-%d")
      jstxt += 'var sumLatestName = "%s";\n' % s3_exe.split("/")[-1]
      jstxt += 'var sumLatestExe = "http://kjkpub.s3.amazonaws.com/%s";\n' % s3_exe
      jstxt += 'var sumLatestPdb = "http://kjkpub.s3.amazonaws.com/%s";\n' % s3_pdb_zip
      jstxt += 'var sumLatestInstaller = "http://kjkpub.s3.amazonaws.com/%s";\n' % s3_installer

    s3UploadFilePublic(installer, s3_installer)
    s3UploadFilePublic(pdb_zip, s3_pdb_zip)
    s3UploadFilePublic(exe, s3_exe)

    if build_prerelease:
      s3UploadDataPublic(jstxt, "sumatrapdf/sumatralatest.js")
      txt = "%s\n" % ver
      s3UploadDataPublic(txt, "sumatrapdf/sumpdf-prerelease-latest.txt")
    else:
      s3UploadFilePublic(exe_zip, s3_exe_zip)
"""

if __name__ == "__main__":
  main()
