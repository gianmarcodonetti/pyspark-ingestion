import argparse
import getpass

from pyspark_ingestion.utils import security


def main(filename, password):
    security.encrypt_json(filename, password)


def create_parser():
    prs = argparse.ArgumentParser(description="Encrypting settings file.")
    prs.add_argument('filename', type=str, nargs='?', help="Filename of the file to encrypt.",
                     default='settings.json')
    return prs


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    _password = getpass.getpass("Encryption key:\n")
    _filename = args.filename
    main(_filename, _password)
