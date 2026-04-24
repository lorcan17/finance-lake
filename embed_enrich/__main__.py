from .client import EmbeddingClient
from .duckdb_conn import connect
from .normalise import process


def main() -> None:
    con = connect()
    client = EmbeddingClient()
    process(con, client)
    con.close()


if __name__ == "__main__":
    main()
