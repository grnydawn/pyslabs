
import os
import pyslabs

here = os.path.dirname(__file__)
slabfile = os.path.join(here, "test.slab")

def f1():
    print("This is f1.")

def f2():
    print("This is f2.")


def main():

    with pyslabs.open(slabfile, "w") as slabs:
        testvar = slabs.define_var("test")
        testvar.write(f1)
        testvar.write(f2)

    with pyslabs.open(slabfile, "r") as slabs:
        testarr = slabs.get_array("test")

    print(type(testarr))
    print(testarr)
    testarr[0]()
    testarr[1]()

    os.remove(slabfile)


if __name__ == "__main__":
    main()
