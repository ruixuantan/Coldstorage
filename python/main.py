from coldstorage import Coldstorage


def main():
    cs = Coldstorage()
    with cs:
        cs.put(b"key", b"value")
        print(cs.get(b"key"))
        cs.delete(b"key")
        print(cs.get(b"key"))


if __name__ == "__main__":
    main()
