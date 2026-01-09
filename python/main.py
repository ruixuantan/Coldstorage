from coldstorage import Coldstorage


def main():
    cs = Coldstorage()
    with cs:
        cs.put(b"apple", b"1")
        cs.put(b"banana", b"2")
        cs.put(b"cherry", b"3")
        cs.put(b"date", b"4")
        cs.put(b"elderberry", b"5")

        print(cs.get(b"apple"))
        print(cs.get(b"non-existent"))

        with cs.scan(b"apple", b"elderberry") as cursor:
            while cursor.is_valid():
                print(cursor.key(), cursor.val())
                cursor.next()


if __name__ == "__main__":
    main()

    import shutil

    shutil.rmtree("coldstorage.db")
