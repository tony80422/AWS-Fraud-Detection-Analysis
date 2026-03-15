import os
import json
import base64
import uuid
import boto3
from datetime import datetime

# AWS clients
runtime = boto3.client("sagemaker-runtime")
s3 = boto3.client("s3")

# Environment variables
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "sagemaker-xgboost-2026-03-13-23-05-26-528")
PREDICTION_BUCKET = os.environ.get("PREDICTION_BUCKET", "finalproject-fraud-detection")
PREDICTION_PREFIX = os.environ.get("PREDICTION_PREFIX", "predictions/realtime")
ENDPOINT_BATCH_SIZE = int(os.environ.get("ENDPOINT_BATCH_SIZE", "500"))
SAVE_ONE_FILE_PER_BATCH = os.environ.get("SAVE_ONE_FILE_PER_BATCH", "true").lower() == "true"
THRESHOLD = float(os.environ.get("THRESHOLD", "0.5"))


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_feature_vector(record):
    """
    Build the feature vector in the exact same order as the offline training data.

    Feature order:
    step, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
    type_CASH_IN, type_CASH_OUT, type_DEBIT, type_PAYMENT, type_TRANSFER
    """
    txn_type = str(record.get("type", "")).upper()

    type_cash_in = 1 if txn_type == "CASH_IN" else 0
    type_cash_out = 1 if txn_type == "CASH_OUT" else 0
    type_debit = 1 if txn_type == "DEBIT" else 0
    type_payment = 1 if txn_type == "PAYMENT" else 0
    type_transfer = 1 if txn_type == "TRANSFER" else 0

    return [
        safe_int(record.get("step", 0)),
        safe_float(record.get("amount", 0.0)),
        safe_float(record.get("oldbalanceOrg", 0.0)),
        safe_float(record.get("newbalanceOrig", 0.0)),
        safe_float(record.get("oldbalanceDest", 0.0)),
        safe_float(record.get("newbalanceDest", 0.0)),
        type_cash_in,
        type_cash_out,
        type_debit,
        type_payment,
        type_transfer,
    ]


def build_csv_payload(feature_rows):
    return "\n".join(
        ",".join(map(str, row))
        for row in feature_rows
    )


def parse_batch_prediction_result(result_text):
    result_text = result_text.strip()

    if not result_text:
        return []

    try:
        parsed = json.loads(result_text)
        if isinstance(parsed, dict) and "predictions" in parsed:
            predictions = parsed["predictions"]
            scores = []
            for item in predictions:
                if isinstance(item, dict) and "score" in item:
                    scores.append(float(item["score"]))
                else:
                    scores.append(float(item))
            return scores
    except Exception:
        pass

    if "\n" in result_text:
        return [float(x.strip()) for x in result_text.split("\n") if x.strip()]

    if "," in result_text:
        return [float(x.strip()) for x in result_text.split(",") if x.strip()]

    return [float(result_text)]


def invoke_endpoint_batch(feature_rows):
    payload = build_csv_payload(feature_rows)

    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="text/csv",
        Body=payload
    )

    result_text = response["Body"].read().decode("utf-8").strip()
    scores = parse_batch_prediction_result(result_text)

    return scores


def save_batch_prediction_results(output_records):
    now = datetime.utcnow()
    file_id = uuid.uuid4().hex
    s3_key = (
        f"{PREDICTION_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{now.strftime('%Y%m%dT%H%M%S')}_{file_id}.jsonl"
    )

    body = "\n".join(json.dumps(record) for record in output_records)

    s3.put_object(
        Bucket=PREDICTION_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    return s3_key


def save_prediction_result(output_record):
    now = datetime.utcnow()
    s3_key = (
        f"{PREDICTION_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{output_record['transaction_id']}.json"
    )

    s3.put_object(
        Bucket=PREDICTION_BUCKET,
        Key=s3_key,
        Body=json.dumps(output_record).encode("utf-8"),
        ContentType="application/json"
    )

    return s3_key


def lambda_handler(event, context):
    valid_records = []
    decode_errors = []

    # Step 1: Decode Kinesis records
    for item in event.get("Records", []):
        try:
            raw_data = base64.b64decode(item["kinesis"]["data"]).decode("utf-8")
            record = json.loads(raw_data)
            valid_records.append(record)
        except Exception as e:
            decode_errors.append({
                "error": f"decode_or_parse_error: {str(e)}"
            })

    if not valid_records:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No valid records to process",
                "errors": decode_errors
            })
        }

    all_results = []
    batch_s3_keys = []

    # Step 2: Invoke endpoint in batches inside one Lambda execution
    for batch_number, record_batch in enumerate(chunked(valid_records, ENDPOINT_BATCH_SIZE), start=1):
        feature_rows = [build_feature_vector(record) for record in record_batch]

        try:
            scores = invoke_endpoint_batch(feature_rows)
        except Exception as e:
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Endpoint batch invocation failed",
                    "error": str(e),
                    "batch_number": batch_number,
                    "record_count": len(record_batch)
                })
            }

        if len(scores) != len(record_batch):
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Prediction count does not match input count",
                    "batch_number": batch_number,
                    "input_count": len(record_batch),
                    "prediction_count": len(scores)
                })
            }

        output_records = []

        for record, features, score in zip(record_batch, feature_rows, scores):
            predicted_label = 1 if float(score) >= THRESHOLD else 0
            endpoint_payload = ",".join(map(str, features))

            output_record = {
                "transaction_id": record.get("transaction_id"),
                "timestamp": record.get("timestamp"),
                "location": record.get("location"),
                "feature_version": record.get("feature_version", "v1"),
                "type": record.get("type"),
                "step": record.get("step"),
                "amount": record.get("amount"),
                "oldbalanceOrg": record.get("oldbalanceOrg"),
                "newbalanceOrig": record.get("newbalanceOrig"),
                "oldbalanceDest": record.get("oldbalanceDest"),
                "newbalanceDest": record.get("newbalanceDest"),
                "actual_isFraud": record.get("actual_isFraud"),
                "predicted_score": float(score),
                "predicted_label": predicted_label,
                "threshold": THRESHOLD,
                "endpoint_name": ENDPOINT_NAME,
                "endpoint_payload": endpoint_payload,
                "processed_at": datetime.utcnow().isoformat() + "Z"
            }
            output_records.append(output_record)

        # Step 3: Save results to S3
        if SAVE_ONE_FILE_PER_BATCH:
            batch_s3_key = save_batch_prediction_results(output_records)
            batch_s3_keys.append(batch_s3_key)

            for output_record in output_records:
                all_results.append({
                    "transaction_id": output_record["transaction_id"],
                    "predicted_score": output_record["predicted_score"],
                    "predicted_label": output_record["predicted_label"],
                    "s3_key": batch_s3_key
                })
        else:
            for output_record in output_records:
                s3_key = save_prediction_result(output_record)
                all_results.append({
                    "transaction_id": output_record["transaction_id"],
                    "predicted_score": output_record["predicted_score"],
                    "predicted_label": output_record["predicted_label"],
                    "s3_key": s3_key
                })

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_count": len(all_results),
            "endpoint_batch_size": ENDPOINT_BATCH_SIZE,
            "save_one_file_per_batch": SAVE_ONE_FILE_PER_BATCH,
            "batch_s3_keys": batch_s3_keys,
            "errors": decode_errors,
            "results": all_results
        })
    }
