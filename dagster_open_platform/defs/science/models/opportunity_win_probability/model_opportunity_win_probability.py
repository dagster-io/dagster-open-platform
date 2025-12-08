import glob
import os
import pickle
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import shap
import snowflake.connector
import xgboost as xgb
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dagster_open_platform.defs.science.models.opportunity_win_probability.utils import (
    _database_from_env,
    _model_data_schema_from_env,
    _prediction_schema_from_env,
)
from dagster_open_platform.utils.environment_helpers import get_environment
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def connect_to_snowflake(database, schema):
    """Establish connection to Snowflake."""
    is_local = get_environment() == "LOCAL"

    if is_local:
        # Use external browser authentication for local development
        return snowflake.connector.connect(
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            authenticator="externalbrowser",
            user=os.environ.get("SNOWFLAKE_USER"),
            warehouse="purina",
            database=database,
            schema=schema,
            role=os.environ.get("SNOWFLAKE_ROLE"),
        )
    else:
        # Use key-pair authentication for non-local environments
        private_key_str = os.environ.get("SNOWFLAKE_DBT_PRIVATE_KEY")
        if not private_key_str:
            raise ValueError(
                "SNOWFLAKE_DBT_PRIVATE_KEY environment variable is required for key-pair authentication"
            )

        # Parse the private key from PEM string
        private_key = serialization.load_pem_private_key(
            private_key_str.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )

        # Convert to PKCS8 format for Snowflake
        private_key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=os.environ.get("SNOWFLAKE_USER"),
            private_key=private_key_pem,
            warehouse="purina",
            database=database,
            schema=schema,
            role=os.environ.get("SNOWFLAKE_ROLE"),
        )


script_dir = os.path.dirname(os.path.abspath(__file__))

# Define features to use
features = {
    "categorical": [
        "industry_standardized",
        "region",
        "latest_funding_stage",
        "win_loss_competitor",
        # 'current_orchestrator',
        # 'account_source',
        # 'owner_name',
        # 'sales_engineer_name',
        # 'account_source_custom',
        # 'sal_created_by_name',
        # 'annual_revenue_bucket',
        # 'employee_count_bucket',
    ],
    "numeric": [
        "total_funding",
        "annual_revenue",
        "number_of_employees",
        "opp_age_at_sal",
        # 'new_arr',
        # 'sal_to_closed_days',
    ],
    "binary": [
        "viewed_pricing_page_before_sal",
        "used_oss_before_sal",
        "attended_webinar_before_sal",
        "github_interaction_before_sal",
        "linkedin_interaction_before_sal",
        "signed_up_for_webinar_before_sal",
        "slack_interaction_before_sal",
        "daggy_u_course_enrollment_before_sal",
        "is_any_meeting_attended_by_executive_before_sal",
        "is_any_meeting_attended_by_manager_before_sal",
        "is_any_meeting_attended_by_sales_engineer_before_sal",
        "user_added_to_organization_before_sal",
        "trial_started_before_sal",
        "is_public",
        # 'is_primary_evaluator_identified',
        # 'within_budget',
        # 'has_budget_risk',
        # 'has_competition_risk',
        # 'has_compliance_risk',
        # 'has_integration_risk',
        # 'has_process_risk',
        # 'has_resource_risk',
        # 'has_security_risk',
        # 'has_stakeholder_risk',
        # 'has_technical_risk',
        # 'has_timeline_risk',
        # 'uses_airbyte',
        # 'uses_bigquery',
        # 'uses_databricks',
        # 'uses_dbt',
        # 'uses_fivetran',
        # 'uses_looker',
        # 'uses_powerbi',
        # 'uses_redshift',
        # 'uses_snowflake',
        # 'uses_tableau',
        # 'is_any_meeting_attended_by_executive',
        # 'is_any_meeting_attended_by_manager',
        # 'is_any_meeting_attended_by_sales_engineer',
    ],
}

# Define columns to exclude from feature preparation
excluded_columns = [
    # 'meeting_count',
    # 'win_loss_competitor',
    # 'region',
    # 'new_arr',
    # 'sal_to_closed_days',
    # 'total_funding',
    # 'annual_revenue',
    # 'number_of_employees',
    # 'within_budget'
]


def fetch_data(conn):
    """Fetch and prepare the initial dataset."""
    conn.cursor().execute(f"USE SCHEMA {_model_data_schema_from_env()}")
    with open(os.path.join(script_dir, "sql/all_data.sql")) as query_file:
        df = pd.read_sql(query_file.read(), conn)
    df.columns = [col.lower() for col in df.columns]
    return df


def prepare_features(df, excluded_columns=None):
    """Prepare features for modeling."""
    if excluded_columns is None:
        excluded_columns = []

    # Filter out excluded columns
    for feature_type in features:
        features[feature_type] = [
            col for col in features[feature_type] if col not in excluded_columns
        ]

    # Process features
    feature_dfs = []

    # Process categorical features with consistent column creation
    if features["categorical"]:
        # Define all expected categorical values for consistent column creation
        expected_categorical_columns = [
            "industry_standardized_Transportation & Logistics",
        ]

        categorical_dfs = []
        for col in features["categorical"]:
            # Get dummies for each categorical column individually
            col_dummies = pd.get_dummies(df[col], prefix=col, dummy_na=False)
            categorical_dfs.append(col_dummies)

        # Combine all categorical dummy variables
        X_categorical = pd.concat(categorical_dfs, axis=1)

        # Add any missing expected columns with zeros
        for expected_col in expected_categorical_columns:
            if expected_col not in X_categorical.columns:
                X_categorical[expected_col] = 0

        feature_dfs.append(X_categorical)

    # Process numeric features
    if features["numeric"]:
        X_numeric = df[features["numeric"]].replace([np.inf, -np.inf], np.nan)
        scaler = StandardScaler()
        X_numeric = pd.DataFrame(scaler.fit_transform(X_numeric), columns=features["numeric"])
        feature_dfs.append(X_numeric)

    # Process binary features
    if features["binary"]:
        X_binary = df[features["binary"]].fillna(False).astype(int)
        feature_dfs.append(X_binary)

    # Combine all features
    X = pd.concat(feature_dfs, axis=1).drop(columns=excluded_columns, errors="ignore")
    y = df["won"].astype(int)

    # Ensure consistent column ordering between training and prediction
    # Sort columns alphabetically to maintain consistent order
    X = X.reindex(sorted(X.columns), axis=1)

    print(X.columns)

    return X, y


XGB_MODEL_PARAMS = {
    "n_estimators": 100,
    "learning_rate": 0.3,
    "max_depth": 7,
    "min_child_weight": 3,
    "gamma": 0.0,
    "reg_alpha": 0.0,
    "reg_lambda": 1.0,
    "colsample_bytree": 0.8,
    "subsample": 0.8,
    "random_state": 42,
    # scale_pos_weight will be set dynamically
    "base_score": 0.5,
    "missing": 1,
    "num_parallel_tree": 1,
    "importance_type": "gain",
}


def train_model(X, y):
    """Train the XGBoost model."""
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Calculate scale_pos_weight based on class imbalance
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    scale_pos_weight = neg_count / pos_count
    print(f"\nClass distribution - Negative: {neg_count}, Positive: {pos_count}")
    print(f"Setting scale_pos_weight to: {scale_pos_weight:.2f}")

    # Create and train XGBoost model
    model_params = XGB_MODEL_PARAMS.copy()
    model_params["scale_pos_weight"] = scale_pos_weight
    model = xgb.XGBClassifier(**model_params)

    print("\nTraining XGBoost model...")
    model.fit(X_train, y_train)
    print("Model training completed")

    # Calculate and print AUC score
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc_score = roc_auc_score(y_test, y_pred_proba)
    print(f"\nAUC Score: {auc_score:.4f}")

    return model, X_test


def get_confidence_score(model, X):
    """Calculate confidence score for predictions."""
    # Get the original model predictions
    original_pred = model.predict_proba(X)[:, 1]

    # Get raw predictions from individual trees for confidence calculation
    dmatrix = xgb.DMatrix(X)
    tree_preds = []

    # Sample trees to get prediction variance
    n_bootstraps = 50
    for _ in range(n_bootstraps):
        # Sample a subset of trees
        n_trees = model.n_estimators // 2  # Use half the trees each time
        tree_indices = np.random.choice(model.n_estimators, size=n_trees, replace=False)
        tree_pred = np.zeros(X.shape[0])

        # Get predictions from sampled trees
        for tree_idx in tree_indices:
            tree_pred += model.get_booster().predict(
                dmatrix, iteration_range=(tree_idx, tree_idx + 1), output_margin=True
            )

        # Average and convert to probability
        tree_pred = tree_pred / n_trees
        tree_pred = 1 / (1 + np.exp(-tree_pred))
        tree_preds.append(tree_pred)

    # Stack predictions
    tree_preds = np.stack(tree_preds, axis=1)

    # Calculate standard deviation of predictions
    std_preds = np.std(tree_preds, axis=1)

    # Calculate confidence based on prediction strength (distance from 0.5)
    decision_confidence = 2 * np.abs(original_pred - 0.5)

    # Calculate confidence based on prediction variance
    # Scale std_preds to be between 0 and 1
    max_std = np.percentile(std_preds, 95)  # Use 95th percentile as max
    variance_confidence = 1 - (std_preds / max_std)
    variance_confidence = np.clip(variance_confidence, 0, 1)

    # Combine both confidence measures
    # Weight decision confidence more heavily
    confidence_scores = 0.7 * decision_confidence + 0.3 * variance_confidence

    return original_pred, confidence_scores


def print_feature_importance(model, X, X_test):
    """Print feature importance for the XGBoost model."""
    # Get feature importance
    feature_importance = pd.DataFrame(
        {"feature": X.columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    print("\nTop 20 Most Important Features:")
    print("=============================")
    for _, row in feature_importance.head(20).iterrows():
        print(f"{row['feature']} | {row['importance']:.4f}")

    # SHAP analysis
    print("\nCalculating SHAP values...")
    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        # Handle both binary and multi-class cases
        if isinstance(shap_values, list):
            shap_values = np.array(shap_values)
            if len(shap_values.shape) == 3:  # Multi-class case
                shap_values = shap_values[1]  # Take the positive class
            else:
                shap_values = shap_values[0]  # Take the first array

        # Calculate mean absolute SHAP values
        mean_shap = np.abs(shap_values).mean(axis=0)

        # Create DataFrame with SHAP values
        shap_df = pd.DataFrame({"feature": X.columns, "mean_shap": mean_shap}).sort_values(
            "mean_shap", ascending=False
        )

        print("\nTop 20 Most Important Features (SHAP Values):")
        print("===========================================")
        for _, row in shap_df.head(20).iterrows():
            print(f"{row['feature']} | {row['mean_shap']:.4f}")
    except Exception as e:
        print(f"Error calculating SHAP values: {e!s}")
        print("Continuing without SHAP analysis...")


def train_and_save_production_model(X, y, output_file=None):
    """Train XGBoost model on full data and save to a pickle file for production use."""
    # Calculate scale_pos_weight based on class imbalance
    neg_count = (y == 0).sum()
    pos_count = (y == 1).sum()
    scale_pos_weight = neg_count / pos_count
    print(f"\n[Production] Class distribution - Negative: {neg_count}, Positive: {pos_count}")
    print(f"[Production] Setting scale_pos_weight to: {scale_pos_weight:.2f}")

    # Create and train XGBoost model
    model_params = XGB_MODEL_PARAMS.copy()
    model_params["scale_pos_weight"] = scale_pos_weight
    model = xgb.XGBClassifier(**model_params)

    print("\n[Production] Training XGBoost model on full data...")
    model.fit(X, y)
    print("[Production] Model training completed.")

    # Save model to pickle file
    if output_file is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(
            current_dir, f"production_xgb_model_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        )
    with open(output_file, "wb") as f:
        pickle.dump(model, f)
    print(f"[Production] Model saved to: {output_file}")
    return output_file


def predict_and_write_to_snowflake(output_table="model_opportunity_win_probability_predictions"):
    """Predict on non-closed opportunities and write results to Snowflake."""
    print("\nStarting prediction and write-to-Snowflake pipeline...")
    # Connect to Snowflake
    print("Connecting to Snowflake...")
    conn = connect_to_snowflake(database=_database_from_env(), schema=_prediction_schema_from_env())
    try:
        # Fetch non-closed opportunities
        df = fetch_data(conn)

        X, y = prepare_features(df, excluded_columns=excluded_columns)
        filtered_df = df[df["split"] == "prediction"].reset_index(drop=True)
        X = X[df["split"] == "prediction"]
        y = y[df["split"] == "prediction"]

        filtered_df = filtered_df.reset_index(drop=True)
        X = X.reset_index(drop=True)
        y = y.reset_index(drop=True)

        print(f"Fetched {len(filtered_df):,} non-closed opportunities.")
        if len(filtered_df) == 0:
            print("No non-closed opportunities to predict on.")
            return

        # Find the latest production model pickle file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        model_files = sorted(
            glob.glob(os.path.join(current_dir, "production_models", "production_xgb_model_*.pkl")),
            reverse=True,
        )
        if not model_files:
            print("No production model pickle file found.")
            return
        model_path = model_files[0]
        print(f"Loading model from: {model_path}")
        with open(model_path, "rb") as f:
            model = pickle.load(f)

        # Filter features to match those the model was trained on
        # Get the feature names the model was trained on
        if hasattr(model, "feature_names_in_"):
            trained_features = list(model.feature_names_in_)
        else:
            # Fallback: get feature names from the model's booster
            trained_features = model.get_booster().feature_names

        # Filter X to only include features that were in training data
        available_features = [col for col in trained_features if col in X.columns]
        missing_features = [col for col in trained_features if col not in X.columns]
        extra_features = [col for col in X.columns if col not in trained_features]

        if missing_features:
            print(f"Warning: Missing features from training data: {missing_features}")
        if extra_features:
            print(f"Info: Removing extra features not in training data: {extra_features}")

        # Filter X to only include trained features
        X_filtered = X[available_features]

        # Make predictions and confidence scores
        base_pred, confidence_scores = get_confidence_score(model, X_filtered)

        # Prepare results DataFrame
        results = pd.DataFrame(
            {
                "opportunity_id": filtered_df["opportunity_id"],
                "predicted_probability": base_pred,
                "confidence_score": confidence_scores,
                "prediction_timestamp": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        # Write results to Snowflake
        print(f"Writing predictions to Snowflake table: {output_table}")
        results.to_csv("results.csv", index=False)
        # Use Snowflake connector for bulk insert
        success = True
        try:
            # Create table if not exists
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {output_table} (
                opportunity_id VARCHAR,
                predicted_probability FLOAT,
                confidence_score FLOAT,
                prediction_timestamp TIMESTAMP_TZ
            )
            """
            with conn.cursor() as cur:
                cur.execute(f"USE SCHEMA {_prediction_schema_from_env()}")
                cur.execute(create_table_sql)

            # Insert data
            insert_sql = f"INSERT INTO {output_table} (opportunity_id, predicted_probability, confidence_score, prediction_timestamp) VALUES (%s, %s, %s, %s)"
            data_tuples = list(results.itertuples(index=False, name=None))

            with conn.cursor() as cur:
                cur.executemany(insert_sql, data_tuples)
            print(f"Successfully wrote {len(results)} predictions to {output_table}.")
        except Exception as e:
            print(f"Error writing to Snowflake: {e!s}")
            success = False
        return success
    finally:
        print("Closing Snowflake connection...")
        conn.close()


def main():
    """Main function to train the XGBoost model."""
    print("Starting model training pipeline...")

    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    try:
        conn = connect_to_snowflake(
            database=_database_from_env(), schema=_model_data_schema_from_env()
        )
        print("Successfully connected to Snowflake")
    except Exception as e:
        print(f"Error connecting to Snowflake: {e!s}")
        return

    try:
        # Fetch and prepare data
        print("\nFetching data from Snowflake...")
        df = fetch_data(conn)
        print(f"Successfully fetched {len(df):,} rows of data")

        # Prepare features
        print("\nPreparing features...")
        X, y = prepare_features(df, excluded_columns=excluded_columns)
        print(f"Features prepared. Shape: {X.shape}")

        filtered_df = df[df["split"] == "training"]
        X = X[df["split"] == "training"]
        y = y[df["split"] == "training"]

        filtered_df = filtered_df.reset_index(drop=True)
        X = X.reset_index(drop=True)
        y = y.reset_index(drop=True)

        # Train model
        model, X_test = train_model(X, y)

        # Print feature importance
        print_feature_importance(model, X, X_test)

        # Get predictions with confidence scores
        base_pred, confidence_scores = get_confidence_score(model, X)

        # Print prediction and confidence score statistics
        print("\nPrediction Statistics:")
        print("=====================")
        print(f"Mean prediction: {np.mean(base_pred):.4f}")
        print(f"Std prediction: {np.std(base_pred):.4f}")
        print(f"Min prediction: {np.min(base_pred):.4f}")
        print(f"Max prediction: {np.max(base_pred):.4f}")
        print("\nConfidence Score Statistics:")
        print("=========================")
        print(f"Mean confidence: {np.mean(confidence_scores):.4f}")
        print(f"Std confidence: {np.std(confidence_scores):.4f}")
        print(f"Min confidence: {np.min(confidence_scores):.4f}")
        print(f"Max confidence: {np.max(confidence_scores):.4f}")

        # Print a few example predictions
        print("\nSample Predictions (first 5):")
        print("============================")
        for i in range(5):
            print(
                f"Prediction: {base_pred[i]:.4f}, Confidence: {confidence_scores[i]:.4f}, Actual: {y[i]}"
            )

        # Create prediction results DataFrame
        predictions = pd.DataFrame(
            {
                "opportunity_id": filtered_df["opportunity_id"],
                "opportunity_name": filtered_df["opportunity_name"],
                "sal_date": filtered_df["sal_date"],
                "actual_outcome": filtered_df["won"],
                "predicted_probability": base_pred,
                "confidence_score": confidence_scores,
            }
        )

        # Save predictions to CSV
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(
            current_dir, f"prediction_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        predictions.to_csv(output_file, index=False)
        print(f"\nPredictions saved to: {output_file}")

        # Train and save production model
        train_and_save_production_model(X, y)

    except Exception as e:
        print(f"\nError during model training: {e!s}")
        import traceback

        traceback.print_exc()
    finally:
        print("\nClosing Snowflake connection...")
        conn.close()
        print("Pipeline completed")


if __name__ == "__main__":
    main()
