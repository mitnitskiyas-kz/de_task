"""Provide realization of the task."""
import yaml
import os
import sys
from pyspark.sql import SparkSession, DataFrame
import tempfile


class DETaskProcess:
    """Main class for the task."""

    def __init__(self, config_file, key):
        """Initialize essential parameters from yaml file."""
        self.translations_filename = None
        self.sellers_filename = None
        self.products_filename = None
        self.order_reviews_filename = None
        self.order_payments_filename = None
        self.order_items_filename = None
        self.geolocation_filename = None
        self.customers_filename = None
        self.spark = None
        self.config_file = config_file
        self.key = key
        print(f'Initializing process with file {self.config_file}')
        with open(self.config_file) as yaml_configuration:
            self.configuration = yaml.load(yaml_configuration, Loader=yaml.FullLoader)

        kaggle_integration = self.configuration.get('kaggle_integration')
        self.username = kaggle_integration.get('username')
        self.dataset_name = kaggle_integration.get('dataset_name')

        data_aliases = self.configuration.get('data_aliases')
        self.customers_filename = data_aliases.get('customers')
        self.geolocation_filename = data_aliases.get('geolocation')
        self.order_items_filename = data_aliases.get('order_items')
        self.order_payments_filename = data_aliases.get('order_payments')
        self.order_reviews_filename = data_aliases.get('order_reviews')
        self.products_filename = data_aliases.get('products')
        self.sellers_filename = data_aliases.get('sellers')
        self.translations_filename = data_aliases.get('translations')
        print(' This process will work with the following params')
        print(f' Kaggle username : {self.username}')
        print(f' Kaggle dataset: {self.dataset_name}')
        print(f' Customers filename: {self.customers_filename}')
        print(f' Geolocation filename: {self.geolocation_filename}')
        print(f' Order items filename: {self.order_items_filename}')
        print(f' Order payments filename: {self.order_payments_filename}')
        print(f' Order reviews filename: {self.order_reviews_filename}')
        print(f' Products filename: {self.products_filename}')
        print(f' Sellers filename: {self.sellers_filename}')
        print(f' Translations filename: {self.translations_filename}')

    def kaggle_integration(self):
        """Perform Kaggle integration and download necessary files."""
        try:
            os.environ['KAGGLE_USERNAME'] = self.username
            os.environ['KAGGLE_KEY'] = self.key
            import kaggle
            temporary_folder = tempfile.gettempdir()
            print(f'Downloading and unzipping into temporary folder started: {temporary_folder}')
            kaggle.api.dataset_download_files(self.dataset_name,
                                              path=temporary_folder, unzip=True)
            print('Downloading and unzipping into temporary folder finished')
        except kaggle.rest.ApiException as e:
            print('Wrong credentials', e)

    def init_spark(self):
        """Initialize spark."""
        self.spark = (SparkSession.builder.appName('DETaskProcess').
                      getOrCreate())
        self.spark.sparkContext.setLogLevel('WARN')

    @staticmethod
    def write_results(df: DataFrame, info_str: str, target_path: str):
        """Write dataframe to target path."""
        print(f'Writing {info_str} to target_path: {target_path}')
        (df.coalesce(1).write.format('csv').option('header', True).option('sep', '~')
         .mode('overwrite').save(target_path))

    def process(self):
        """Calculate main logic."""
        pass

    def finish(self):
        """Stop spark context."""
        self.spark.stop()


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) == 1:
        myDETaskProcess = DETaskProcess(os.path.join(os.getcwd(), 'cfg', 'config.yaml'),
                                        '53f4bee90070f4af669078622009e336')
    else:
        myDETaskProcess = DETaskProcess(sys.argv[1], sys.argv[2])
    myDETaskProcess.kaggle_integration()
    myDETaskProcess.init_spark()
    myDETaskProcess.process()
    myDETaskProcess.finish()
    exit(0)
