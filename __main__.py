from pipelines.example import ExamplePipeline
from modules import processed_files
import polars

def main():
    processed_files.init_table()
    ExamplePipeline().run()
    
    print(polars.read_delta("local/silver/meta_processed_files"))
    print(polars.read_delta("local/silver/ExampleModel"))

if __name__ == "__main__":
    main()
