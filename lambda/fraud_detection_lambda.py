import os
import json
import base64
import boto3
from datetime import datetime

# AWS clients
runtime = boto3.client("sagemaker-runtime")
s3 = boto3.client("s3")

# Environment variables
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "sagemaker-xgboost-2026-03-13-23-05-26-528")
PREDICTION_BUCKET = os.environ.get("PREDICTION_BUCKET", "finalproject-fraud-detection")
PREDICTION_PREFIX = os.environ.get("PREDICTION_PREFIX", "predictions/realtime")

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
    """
    Convert multiple feature rows into one multi-line CSV payload.
    One row = one inference sample.
    """
    return "\n".join(
        ",".join(map(str, row))
        for row in feature_rows
    )

def parse_batch_prediction_result(result_text):
    """
    Parse SageMaker endpoint batch inference response.

    Supported formats:
    1. Plain float for a single row: "0.12345"
    2. Newline-separated floats:
       0.123
       0.456
    3. Comma-separated floats: "0.123,0.456"
    4. JSON format:
       {"predictions":[{"score":0.123},{"score":0.456}]}
       or {"predictions":[0.123,0.456]}
    """
    result_text = result_text.strip()

    if not result_text:
        return []

    # Try JSON first
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

    # Newline-separated scores
    if "\n" in result_text:
        return [float(x.strip()) for x in result_text.split("\n") if x.strip()]

    # Comma-separated scores
    if "," in result_text:
        return [float(x.strip()) for x in result_text.split(",") if x.strip()]

    # Single score
    return [float(result_text)]

def invoke_endpoint_batch(feature_rows):
    """
    Invoke the SageMaker endpoint once for a batch of records.
    """
    payload = build_csv_payload(feature_rows)

    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="text/csv",
        Body=payload
    )

    result_text = response["Body"].read().decode("utf-8").strip()
    scores = parse_batch_prediction_result(result_text)

    return scores, payload

def save_prediction_result(output_record):
    """
    Save one prediction result to S3 in JSON format.
    """
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
    feature_rows = []
    decode_errors = []

    # Step 1: Decode Kinesis records and build features
    for item in event.get("Records", []):
        try:
            raw_data = base64.b64decode(item["kinesis"]["data"]).decode("utf-8")
            record = json.loads(raw_data)
            features = build_feature_vector(record)

            valid_records.append(record)
            feature_rows.append(features)

        except Exception as e:
            decode_errors.append({
                "error": f"decode_or_feature_error: {str(e)}"
            })

    # No valid records
    if not valid_records:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No valid records to process",
                "errors": decode_errors
            })
        }

    # Step 2: Invoke endpoint once for the whole batch
    try:
        scores, batch_payload = invoke_endpoint_batch(feature_rows)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Endpoint batch invocation failed",
                "error": str(e),
                "record_count": len(valid_records)
            })
        }

    # Step 3: Validate returned score count
    if len(scores) != len(valid_records):
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Prediction count does not match input count",
                "input_count": len(valid_records),
                "prediction_count": len(scores)
            })
        }

    # Step 4: Save each prediction result
    results = []

    for record, features, score in zip(valid_records, feature_rows, scores):
        predicted_label = 1 if float(score) >= 0.5 else 0
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
            "threshold": 0.5,
            "endpoint_name": ENDPOINT_NAME,
            "endpoint_payload": endpoint_payload,
            "processed_at": datetime.utcnow().isoformat() + "Z"
        }

        s3_key = save_prediction_result(output_record)

        results.append({
            "transaction_id": output_record["transaction_id"],
            "predicted_score": float(score),
            "predicted_label": predicted_label,
            "s3_key": s3_key
        })

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_count": len(results),
            "errors": decode_errors,
            "results": results
        })
    }