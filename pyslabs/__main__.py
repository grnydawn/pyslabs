"""main entry for pyslabs command-line interface"""

import pyslabs

def cmd_info(args):

    with pyslabs.open(args.slabfile) as fp:

        if args.list:
            print(", ".join(fp.info("list", verbose=args.verbose)))
  
        elif args.slab:
            print("") 
            for var, (n, t, x, m) in fp.info("slab", args.var, verbose=args.verbose).items():
                print(var) 
                print("   slab count: %d" % n) 
                print("  total bytes: %d" % t) 
                print("    max bytes: %d" % x) 
                print("    min bytes: %d" % m) 

        elif args.var:
            var = fp.info("var", args.var, verbose=args.verbose)

        else:
            for k, v in fp.info("", args.var, verbose=args.verbose):
                if k == "size":
                    print("size: %d bytes" % v)

                elif k == "dims":
                    buf = []
                    for n, l in v:
                        buf.append("%s(%d)" % (n, l))
                    print("dims: " + ", ".join(buf))

                elif k == "vars":
                    buf = []
                    for n, s in v:
                        buf.append("%s(%s)" % (n, ", ".join([str(_s) for _s in s])))
                    print("vars: " + ", ".join(buf))
                   

def main():
    import argparse
    from pyslabs.info import version

    parser = argparse.ArgumentParser(description="pyslabs command-line tool")
    parser.add_argument("--version", action="version", version="pyslabs "+version)
    parser.add_argument("--verbose", action="store_true", help="verbose info")

    cmds = parser.add_subparsers(title='subcommands',
                description='pyslabs subcommands', help='additional help')

    p_info = cmds.add_parser('info')
    p_info.add_argument("slabfile", help="slabfile path")
    p_info.add_argument("-l", "--list", action="store_true", help="a list of variables")
    p_info.add_argument("-v", "--var", help="variable info")
    p_info.add_argument("-s", "--slab", action="store_true", help="slab info")
    p_info.set_defaults(func=cmd_info)

    argps = parser.parse_args()
    argps.func(argps)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
