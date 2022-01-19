import json
import os
import shutil
import sys
import time
import pandas as pd
from botocore.exceptions import ClientError


class StatusIndicator:
    def __init__(self):
        self.previous_status = None
        self.need_newline = False

    def update(self, status):
        if self.previous_status != status:
            if self.need_newline:
                sys.stdout.write("\n")
            sys.stdout.write(status + " ")
            self.need_newline = True
            self.previous_status = status
        else:
            sys.stdout.write(".")
            self.need_newline = True
        sys.stdout.flush()

    def end(self):
        if self.need_newline:
            sys.stdout.write("\n")


def create_fcst_dataset_group(fcst_client, dataset_group_name, domain, logger=None):
    try:
        response = fcst_client.create_dataset_group(
            DatasetGroupName=dataset_group_name,
            Domain=domain.upper(),
        )
        dataset_group_arn = response["DatasetGroupArn"]

        return dataset_group_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            msg = "Forecast dataset group already exists."
            print(msg) if logger is None else logger.info(msg)
            dataset_group_arn = error.response["Message"].split(": ")[-1]

            return dataset_group_arn

        else:
            msg = "An error occurred while communicating with Forecast."
            print(msg) if logger is None else logger.info(msg)
            raise RuntimeError


def create_fcst_dataset(
    fcst_client,
    domain,
    dataset_type,
    dataset_name,
    data_freq,
    schema,
    logger=None,
):
    try:
        response = fcst_client.create_dataset(
            Domain=domain.upper(),
            DatasetType=dataset_type.upper(),
            DatasetName=dataset_name,
            DataFrequency=data_freq.upper(),
            Schema=schema,
        )
        dataset_arn = response["DatasetArn"]

        return dataset_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            msg = "Forecast dataset already exists."
            print(msg) if logger is None else logger.info(msg)
            dataset_arn = error.response["Message"].split(": ")[-1]

            return dataset_arn

        else:
            msg = "An error occurred while communicating with Forecast."
            print(msg) if logger is None else logger.info(msg)
            raise RuntimeError


def create_fcst_dataset_import_job(
    fcst_client,
    dataset_import_job_name,
    dataset_arn,
    dataset_import_path,
    role_arn,
    timestamp_format,
):
    try:
        response = fcst_client.create_dataset_import_job(
            DatasetImportJobName=dataset_import_job_name,
            DatasetArn=dataset_arn,
            DataSource={"S3Config": {"Path": dataset_import_path, "RoleArn": role_arn}},
            TimestampFormat=timestamp_format,
        )

        dataset_import_job_arn = response["DatasetImportJobArn"]

        wait(
            lambda: fcst_client.describe_dataset_import_job(
                DatasetImportJobArn=dataset_import_job_arn
            )
        )

        return dataset_import_job_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast dataset import job was already done.")
            dataset_import_job_arn = error.response["Message"].split(": ")[-1]

            return dataset_import_job_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError


def create_fcst_auto_predictor(
    fcst_client,
    predictor_name,
    fcst_horizon,
    fcst_types,
    fcst_freq,
    dataset_group_arn,
    attr_configs,
    opt_metric,
):
    try:
        response = fcst_client.create_auto_predictor(
            PredictorName=predictor_name,
            ForecastHorizon=fcst_horizon,
            ForecastTypes=fcst_types,
            ForecastFrequency=fcst_freq,
            DataConfig={
                "DatasetGroupArn": dataset_group_arn,
                "AttributeConfigs": attr_configs,
            },
            OptimizationMetric=opt_metric,
            ExplainPredictor=True,
        )

        predictor_arn = response["PredictorArn"]

        wait(lambda: fcst_client.describe_auto_predictor(PredictorArn=predictor_arn))

        return predictor_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast predictor already exists.")

            response = fcst_client.list_predictors()
            predictors = response["Predictors"]
            while "NextToken" in response:
                response = fcst_client.list_predictors(NextToken=response["NextToken"])
                predictors.extend(response["Predictors"])

            for predictor in predictors:
                if predictor["PredictorName"] == predictor_name:
                    predictor_arn = predictor["PredictorArn"]

            return predictor_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError
            
            
def create_fcst_backtest_export(
    fcst_client, backtest_export_name, predictor_arn, backtest_export_path, role_arn
):
    try:
        response = fcst_client.create_predictor_backtest_export_job(
            PredictorBacktestExportJobName=backtest_export_name,
            PredictorArn=predictor_arn,
            Destination={
                "S3Config": {"Path": backtest_export_path, "RoleArn": role_arn}
            },
        )

        backtest_export_arn = response["PredictorBacktestExportJobArn"]

        wait_callback(
            lambda: fcst_client.describe_predictor_backtest_export_job(
                PredictorBacktestExportJobArn=backtest_export_arn
            )
        )

        return backtest_export_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast backtest export job was already done.")
            backtest_export_arn = error.response["Message"].split(": ")[-1]

            return backtest_export_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError


def create_fcst_expl_export(
    fcst_client, expl_export_name, expl_arn, expl_export_path, role_arn
):
    try:
        response = fcst_client.create_explainability_export(
            ExplainabilityExportName=expl_export_name,
            ExplainabilityArn=expl_arn,
            Destination={"S3Config": {"Path": expl_export_path, "RoleArn": role_arn}},
        )

        expl_export_arn = response["ExplainabilityExportArn"]

        wait_callback(
            lambda: fcst_client.describe_explainability_export(
                ExplainabilityExportArn=expl_export_arn
            )
        )

        return expl_export_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast explainability export job was already done.")
            expl_export_arn = error.response["Message"].split(": ")[-1]

            return expl_export_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError
            
            
def create_fcst_fcst(fcst_client, fcst_name, predictor_arn):
    try:
        response = fcst_client.create_forecast(
            ForecastName=fcst_name,
            PredictorArn=predictor_arn,
        )

        fcst_arn = response["ForecastArn"]

        wait_callback(lambda: fcst_client.describe_forecast(ForecastArn=fcst_arn))

        return fcst_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast forecasts already exists.")
            fcst_arn = error.response["Message"].split(": ")[-1]

            return fcst_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError
            
            
def create_fcst_fcst_export(
    fcst_client, fcst_export_name, fcst_arn, fcst_export_path, role_arn
):
    try:
        response = fcst_client.create_forecast_export_job(
            ForecastExportJobName=fcst_export_name,
            ForecastArn=fcst_arn,
            Destination={"S3Config": {"Path": fcst_export_path, "RoleArn": role_arn}},
        )

        fcst_export_arn = response["ForecastExportJobArn"]

        wait_callback(
            lambda: fcst_client.describe_forecast_export_job(ForecastArn=fcst_arn)
        )

        return fcst_export_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast forecast export job was already done.")
            fcst_export_arn = error.response["Message"].split(": ")[-1]

            return fcst_export_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError
            
            
def create_fcst_expl(
    fcst_client,
    expl_name,
    fcst_arn,
    expl_item_subset_path,
    role_arn,
    start_datetime,
    end_datetime,
    specify_time_points=False,
):
    if specify_time_points:
        time_point_granularity = "SPECIFIC"
    else:
        time_point_granularity = "ALL"

    try:
        response = fcst_client.create_explainability(
            ExplainabilityName=expl_name,
            ResourceArn=fcst_arn,
            ExplainabilityConfig={
                "TimeSeriesGranularity": "SPECIFIC",
                "TimePointGranularity": time_point_granularity,
            },
            DataSource={
                "S3Config": {"Path": expl_item_subset_path, "RoleArn": role_arn}
            },
            Schema={
                "Attributes": [
                    {
                        "AttributeName": "item_id",
                        "AttributeType": "string",
                    }
                ]
            },
            StartDateTime=start_datetime,
            EndDateTime=end_datetime,
            EnableVisualization=True,
        )

        expl_arn = response["ExplainabilityArn"]
        
        wait_callback(
            lambda: fcst_client.describe_explainability(ExplainabilityArn=expl_arn)
        )
        
        return expl_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("Forecast explainability already exists.")
            expl_arn = error.response["Message"].split(": ")[-1]

            return expl_arn

        else:
            print("An error occurred while communicating with Forecast.")
            raise RuntimeError


def create_iam_role(
    iam_resource, role_name, assume_role_policy_document, policy_names, logger=None
):
    try:
        response = iam_resource.meta.client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
        )
        role_arn = response["Role"]["Arn"]

        for policy_name in policy_names:
            iam_resource.meta.client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=f"arn:aws:iam::aws:policy/{policy_name}",
            )

        msg = "IAM role creation succeed."
        print(msg) if logger is None else logger.info(msg)

        return role_arn

    except ClientError as error:
        if error.response["Error"]["Code"] == "EntityAlreadyExists":
            msg = "IAM role already exists."
            print(msg) if logger is None else logger.info(msg)

            role_arn = iam_resource.meta.client.get_role(RoleName=role_name)["Role"][
                "Arn"
            ]
            return role_arn

        else:
            msg = "An error occurred while communicating with IAM."
            print(msg) if logger is None else logger.info(msg)
            raise RuntimeError


def create_s3_bucket(s3_resource, region_name, bucket, logger=None):
    try:
        response = s3_resource.meta.client.head_bucket(Bucket=bucket)
        msg = "S3 bucket already exists."
        print(msg) if logger is None else logger.info(msg)

    except ClientError as error:
        if error.response["Error"]["Code"] == "404":
            _ = s3_resource.meta.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
            msg = "S3 bucket creation succeed."
            print(msg) if logger is None else logger.info(msg)

        else:
            msg = "An error occurred while communicating with S3."
            print(msg) if logger is None else logger.info(msg)
            raise RuntimeError


def get_s3_file_uri(bucket, prefix, file_name=None):
    if file_name is None:
        return "s3://" + bucket + "/" + prefix
    else:
        return "s3://" + bucket + "/" + prefix + "/" + file_name


def read_export(s3_resource, bucket_name, prefix, local_path):
    bucket = s3_resource.Bucket(bucket_name)
    depth = len(prefix.split("/")) - 1

    if os.path.exists(local_path) and os.path.isdir(local_path):
        shutil.rmtree(local_path)
    if not (os.path.exists(local_path) and os.path.isdir(local_path)):
        os.makedirs(local_path)

    part_file_name = ""
    part_files = list(bucket.objects.filter(Prefix=prefix))

    for file in part_files:
        if "csv" in file.key:
            part_file_name = file.key.split("/")[depth]
            window_object = s3_resource.Object(bucket_name, file.key)
            file_size = window_object.content_length
            if file_size > 0:
                s3_resource.Bucket(bucket_name).download_file(
                    file.key, local_path + "/" + part_file_name
                )

    temp_dfs = []
    for entry in os.listdir(local_path):
        if os.path.isfile(os.path.join(local_path, entry)):
            df = pd.read_csv(os.path.join(local_path, entry), index_col=None, header=0)
            temp_dfs.append(df)

    df = pd.concat(temp_dfs, axis=0, ignore_index=True, sort=False)
    return df


def upload_dir(s3_resource, local_path, bucket, prefix, logger=None):
    for root, _, files in os.walk(local_path):
        for file in files:
            s3_resource.meta.client.upload_file(
                os.path.join(root, file), bucket, prefix + "/" + file
            )

            file_uri = get_s3_file_uri(bucket, prefix, file)
            msg = f"{file_uri} has been uploaded."
            print(msg) if logger is None else logger.info(msg)


def wait_callback(callback, time_interval=60):
    status_indicator = StatusIndicator()

    while True:
        status = callback()["Status"]
        status_indicator.update(status)
        if status in ("ACTIVE", "CREATE_FAILED"):
            break
        time.sleep(time_interval)

    status_indicator.end()

    return status == "ACTIVE"
