from dotenv import load_dotenv

from csv_extractor_simple.extractor import main

load_dotenv(".env")

if __name__ == "__main__":
    main()
