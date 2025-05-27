import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import roc_auc_score
import shap
import snowflake.connector
import os

def connect_to_snowflake():
    """Establish connection to Snowflake"""
    return snowflake.connector.connect(
        account='AX61354-NA94824',
        authenticator='externalbrowser',
        user='anil.maharjan@dagsterlabs.com',
        warehouse='purina',
        database='sandbox',
        schema='anil',
        role='data_analyst'
    )

def fetch_data(conn):
    """Fetch and prepare the initial dataset"""
    query = """
        SELECT 
        *
        FROM model_opportunity_win_probability
        WHERE 
        sal_date IS NOT NULL
        and new_arr_bucket != 'NULL'
        and is_closed = true
        and opportunity_type = 'New Business'
        and date(created_at) > '2023-06-30'
    """
    df = pd.read_sql(query, conn)
    df.columns = [col.lower() for col in df.columns]
    return df

def prepare_features(df, excluded_columns=None):
    """Prepare features for modeling"""
    if excluded_columns is None:
        excluded_columns = []
    
    # Define features to use
    features = {
        'categorical': [
            'industry_standardized',
            'lead_source',
            'region',
            'latest_funding_stage',
            'win_loss_competitor', 
            # 'current_orchestrator',
            # 'account_source',
            # 'owner_name',
            # 'sales_engineer_name',
            # 'account_source_custom',
            # 'sal_created_by_name',
            # 'annual_revenue_bucket',
            # 'employee_count_bucket',
        ],
        'numeric': [
            'total_funding',
            'annual_revenue',
            'number_of_employees',
            'opp_age_at_sal'
            # 'new_arr',
            # 'sal_to_closed_days',
        ],
        'binary': [
            'viewed_pricing_page_before_sal',
            'used_oss_before_sal',
            'attended_webinar_before_sal',
            'github_interaction_before_sal',
            'linkedin_interaction_before_sal',
            'signed_up_for_webinar_before_sal',
            'slack_interaction_before_sal',
            'daggy_u_course_enrollment_before_sal',
            'is_any_meeting_attended_by_executive_before_sal',
            'is_any_meeting_attended_by_manager_before_sal',
            'is_any_meeting_attended_by_sales_engineer_before_sal',
            'user_added_to_organization_before_sal',
            'trial_started_before_sal',
            'is_public'
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
        ]
    }
    
    # Filter out excluded columns
    for feature_type in features:
        features[feature_type] = [col for col in features[feature_type] 
                                if col not in excluded_columns]
    
    # Process features
    feature_dfs = []
    
    # Process categorical features
    if features['categorical']:
        X_categorical = pd.get_dummies(df[features['categorical']])
        feature_dfs.append(X_categorical)
    
    # Process numeric features
    if features['numeric']:
        X_numeric = df[features['numeric']].replace([np.inf, -np.inf], np.nan)
        scaler = StandardScaler()
        X_numeric = pd.DataFrame(
            scaler.fit_transform(X_numeric), 
            columns=features['numeric']
        )
        feature_dfs.append(X_numeric)
    
    # Process binary features
    if features['binary']:
        X_binary = df[features['binary']].fillna(False).astype(int)
        feature_dfs.append(X_binary)
    
    # Combine all features
    X = pd.concat(feature_dfs, axis=1)
    y = df['won'].astype(int)
    
    return X, y

def train_model(X, y):
    """Train the XGBoost model"""
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
    model = xgb.XGBClassifier(
        n_estimators=100,
        learning_rate=0.3,
        max_depth=7,
        min_child_weight=3,
        gamma=0.0,
        reg_alpha=0.0,
        reg_lambda=1.0,
        colsample_bytree=0.8,
        subsample=0.8,
        random_state=42,
        scale_pos_weight=scale_pos_weight,
        base_score=0.5,
        missing=1,
        num_parallel_tree=1,
        importance_type='gain'
    )
    
    print("\nTraining XGBoost model...")
    model.fit(X_train, y_train)
    print("Model training completed")
    
    # Calculate and print AUC score
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc_score = roc_auc_score(y_test, y_pred_proba)
    print(f"\nAUC Score: {auc_score:.4f}")
    
    return model, X_test

def get_confidence_score(model, X):
    """Calculate confidence score for predictions"""
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
                dmatrix,
                iteration_range=(tree_idx, tree_idx + 1),
                output_margin=True
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
    """Print feature importance for the XGBoost model"""
    # Get feature importance
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 20 Most Important Features:")
    print("=============================")
    for idx, row in feature_importance.head(20).iterrows():
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
        shap_df = pd.DataFrame({
            'feature': X.columns,
            'mean_shap': mean_shap
        }).sort_values('mean_shap', ascending=False)
        
        print("\nTop 20 Most Important Features (SHAP Values):")
        print("===========================================")
        for idx, row in shap_df.head(20).iterrows():
            print(f"{row['feature']} | {row['mean_shap']:.4f}")
    except Exception as e:
        print(f"Error calculating SHAP values: {str(e)}")
        print("Continuing without SHAP analysis...")

def main():
    """Main function to train the XGBoost model"""
    print("Starting model training pipeline...")
    
    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    try:
        conn = connect_to_snowflake()
        print("Successfully connected to Snowflake")
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return
    
    try:
        # Fetch and prepare data
        print("\nFetching data from Snowflake...")
        df = fetch_data(conn)
        print(f"Successfully fetched {len(df):,} rows of data")
        
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
        
        # Prepare features
        print("\nPreparing features...")
        X, y = prepare_features(df, excluded_columns=excluded_columns)
        print(f"Features prepared. Shape: {X.shape}")
        
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
            print(f"Prediction: {base_pred[i]:.4f}, Confidence: {confidence_scores[i]:.4f}, Actual: {y[i]}")
        
        # Create prediction results DataFrame
        predictions = pd.DataFrame({
            'opportunity_id': df['opportunity_id'],
            'opportunity_name': df['opportunity_name'],
            'sal_date': df['sal_date'],
            'actual_outcome': df['won'],
            'predicted_probability': base_pred,
            'confidence_score': confidence_scores
        })
        
        # Save predictions to CSV
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(current_dir, f'prediction_results_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv')
        predictions.to_csv(output_file, index=False)
        print(f"\nPredictions saved to: {output_file}")
        
    except Exception as e:
        print(f"\nError during model training: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nClosing Snowflake connection...")
        conn.close()
        print("Pipeline completed")

if __name__ == "__main__":
    main() 