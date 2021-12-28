from lab_data_logger.services import start_service, RandomNumberService


def main():
    start_service(RandomNumberService, port=18813)


if __name__ == "__main__":
    main()
