import rpyc


def main():
    service = rpyc.connect("localhost", 18813)
    for _ in range(3):
        print(service.root.get_data())


if __name__ == "__main__":
    main()
