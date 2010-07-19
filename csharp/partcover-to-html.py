import os
import os.path
import sys

def usage_and_exit():
    print("Usage: partcover-to-html.py PARTCOVER_FILE.XML HTML_OUT_DIR")
    sys.exit(1)

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

if __name__ == "__main__":
    main()
