from os import listdir, remove
from os.path import join

import variables as var

def main():
    files_list = listdir(var.DIR_TEMP)
    for file in files_list:
        remove(join(var.DIR_TEMP,file))

if __name__ == '__main__':
    main()
