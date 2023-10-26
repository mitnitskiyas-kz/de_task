"""Provide realization of the task."""
import yaml
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, desc, col, expr
import tempfile
from de_task.structure import olist_order_items_dataset_schema
from de_task.calc_rouitine import align_to_weekends


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

        data_export = self.configuration.get('data_export')
        self.path_to_export = data_export.get('path_to_export')
        self.partitioned_by = data_export.get('partitioned_by')
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
        print(' ----------------------------------------------------')
        print(f' Output will be placed here: {self.path_to_export}')
        print(f' Output is partitioned by: {self.partitioned_by}')

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
    def write_results(df: DataFrame, partitioned_by: str, target_path: str):
        """Write dataframe to target path."""
        print(f'Writing to target_path: {target_path}')
        (df.coalesce(1).write.mode('overwrite').partitionBy(partitioned_by).save(target_path))

    def process(self):
        """Calculate main logic."""
        order_items = self.spark.read.format("csv").option("header", "true") \
            .schema(olist_order_items_dataset_schema) \
            .load(os.path.join(tempfile.gettempdir(), self.order_items_filename))
        order_items_aligned = align_to_weekends(order_items, 'shipping_limit_date', 'aligned_date')

        product_aggregates = order_items_aligned.groupby('aligned_date', 'product_id') \
            .agg(count('*').alias('order_by_product_cnt'),
                 sum('price').alias('sum_by_product'))

        week_aggregates = order_items_aligned.groupby('aligned_date') \
            .agg(count('*').alias('week_cnt'),
                 sum('price').alias('week_sum'))

        output_aggregates = product_aggregates.join(week_aggregates, ['aligned_date'], how='inner') \
            .select(col('aligned_date').alias('week_date'),
                    col('product_id'),
                    col('order_by_product_cnt'),
                    col('sum_by_product'),
                    expr('100 * order_by_product_cnt / week_cnt').alias('week_percent_cnt'),
                    expr('100 * sum_by_product / week_sum').alias('week_percent_sum')
                    ).sort('aligned_date', desc('week_sum'))
        self.write_results(output_aggregates, self.partitioned_by, self.path_to_export)

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
