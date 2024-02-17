## for command line ###
from . import PLODResource
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Interact with the P-LOD triplestore.')
    parser.add_argument('-m', '--method')
    parser.add_argument('arg_r')

    args = parser.parse_args()

    r = PLODResource(args.arg_r)

    if args.method:
        method = args.method
        print(getattr(r, method)())
    else:
        print(r.label)
