from met_extractor.extractor import extractor


def main() -> None:
    with extractor:
        extractor.run()


if __name__ == "__main__":
    main()
