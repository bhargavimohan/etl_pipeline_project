import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler


def drop_rows(df, rows_to_drop, processed_data_path):
    # specific to this dataset
    try:
        for col, values in rows_to_drop.items():
            if col in df.columns:
                df_processed = df[~df[col].isin(values)]
        # df = df[df["Gender"] != "Prefer not to say"]
        df_processed.to_csv(processed_data_path, index=False)
        print(
            f"Processed data after dropping some rows saved to: {processed_data_path}"
        )
    except Exception as e:
        print(f"Error saving processed data: {e}")
    return df_processed


def drop_columns(df, columns_to_drop, processed_data_path):
    # drop columns that are least important
    try:
        columns_to_drop = columns_to_drop.split(",") if columns_to_drop else []
        if columns_to_drop:
            df_processed = df.drop(columns=columns_to_drop)
            df_processed.to_csv(processed_data_path, index=False)
            print(
                f"Processed data after dropping unwanted columns saved to: {processed_data_path}"
            )
    except Exception as e:
        print(f"Error saving processed data: {e}")
    return df_processed


def check_nan(df, processed_data_path):
    try:
        # Define the specific replacements for two columns
        # Replace values in the 'Physical_Activity' column for specific rows
        df["Physical_Activity"] = (
            df["Physical_Activity"].replace("None", "Sedentary").fillna("Sedentary")
        )
        # Replace values in the 'Mental_Health_Condition' column for specific rows
        df["Mental_Health_Condition"] = (
            df["Mental_Health_Condition"].replace("None", "Balanced").fillna("Balanced")
        )

        if df.isna().sum().sum() > 0:  # check for NaN
            print("NaN values found. Cleaning data...")
            print(df.isna().sum())
            df_cleaned = df.dropna()  # Drop rows with NaN values
            df_cleaned.to_csv(processed_data_path, index=False)
            print(f"Cleaned data saved to {processed_data_path}")

        else:
            print("No NaN values found.")
            os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)
            df.to_csv(processed_data_path, index=False)
            print(
                f"Processed data after taking care of None values saved to: {processed_data_path}"
            )
    except Exception as e:
        print(f"Error saving processed data: {e}")

    # Grouping and Aggregating, save unique combo

    # workforce demographics - Group by Job_Role, Industry, Gender, Region
    # productivity insights - Group by Work_Location, Company_Support_for_Remote_Work
    # Mental health focus- Group by Mental_Health_Condition, Access_to_Mental_Health_Resources
    # Sleep & lifestyle effects - Group by Sleep_Quality, Physical_Activity

    # Calculate future stress level at the time of retirement


def generate_insights(df, processed_data_path):
    try:
        RETIREMENT_AGE = 65
        df["Years_Left"] = RETIREMENT_AGE - df["Age"]

        physical_activity_map = {"Sedentary": 1, "Weekly": 2, "Daily": 3}
        sleep_quality_map = {"Poor": 1, "Average": 2, "Good": 3}
        mental_health_access_map = {"No": 1, "Limited": 2, "Yes": 3}
        mental_health_condition_map = {
            "Balanced": 5,
            "Anxiety": 3,
            "Burnout": 2,
            "Depression": 1,
        }

        df["Physical_Activity_Score"] = df["Physical_Activity"].map(
            physical_activity_map
        )
        df["Sleep_Quality_Score"] = df["Sleep_Quality"].map(sleep_quality_map)
        df["Mental_Health_Access_Score"] = df["Access_to_Mental_Health_Resources"].map(
            mental_health_access_map
        )
        df["Mental_Health_Condition_Score"] = df["Mental_Health_Condition"].map(
            mental_health_condition_map
        )

        df["Future_Happiness_Index"] = (
            (
                df["Physical_Activity_Score"]
                + df["Sleep_Quality_Score"]
                + df["Mental_Health_Access_Score"]
                + df["Mental_Health_Condition_Score"]
            )
            / 4
        ) * (
            df["Years_Left"] / 65
        )  # Compute Happiness/Stress Index Projection
        df["Future_Happiness_Index"] = df["Future_Happiness_Index"].round(2)
        df["Future_Stress_Level"] = 1 - df["Future_Happiness_Index"]
        scaler = MinMaxScaler(
            feature_range=(0, 1)
        )  # Initialize MinMaxScaler to scale between 0 and 1

        df[["Future_Happiness_Index", "Future_Stress_Level"]] = scaler.fit_transform(
            df[["Future_Happiness_Index", "Future_Stress_Level"]]
        )  # Apply MinMaxScaler to scale the 'Future_Happiness_Index' and 'Future_Stress_Level' columns

        df[["Future_Happiness_Index", "Future_Stress_Level"]] = df[
            ["Future_Happiness_Index", "Future_Stress_Level"]
        ].clip(
            0, 1
        )  # Clip values to the range 0 to 1

        df[["Future_Happiness_Index", "Future_Stress_Level"]] = df[
            ["Future_Happiness_Index", "Future_Stress_Level"]
        ].round(
            2
        )  # Round the final values to 2 decimal points

        df.to_csv(processed_data_path, index=False)
        print(
            f"Processed data after generating new columns saved to: {processed_data_path}"
        )
    except Exception as e:
        print(f"Error saving processed data: {e}")


def generate_insights_using_ML_model(df, processed_data_path):
    pass
    # import pandas as pd
    # from sklearn.model_selection import train_test_split
    # from sklearn.preprocessing import StandardScaler
    # from sklearn.ensemble import RandomForestRegressor
    # from sklearn.metrics import mean_absolute_error, r2_score

    # # Define mappings
    # physical_activity_map = {"Sedentary": 1, "Moderate": 2, "High": 3}
    # sleep_quality_map = {"Poor": 1, "Average": 2, "Good": 3}
    # mental_health_access_map = {"No": 1, "Limited": 2, "Yes": 3}
    # mental_health_condition_map = {"None": 5, "Anxiety": 3, "Burnout": 2, "Depression": 1}

    # # Apply mappings
    # df["Physical_Activity_Score"] = df["Physical_Activity"].map(physical_activity_map)
    # df["Sleep_Quality_Score"] = df["Sleep_Quality"].map(sleep_quality_map)
    # df["Mental_Health_Access_Score"] = df["Access_to_Mental_Health_Resources"].map(mental_health_access_map)
    # df["Mental_Health_Condition_Score"] = df["Mental_Health_Condition"].map(mental_health_condition_map)

    # # Compute remaining working years
    # df["Years_Left"] = 65 - df["Age"]
    # # Select features (X) and target variable (y)
    # X = df[["Years_Left", "Physical_Activity_Score", "Sleep_Quality_Score",
    #         "Mental_Health_Access_Score", "Mental_Health_Condition_Score"]]
    # y = df["Future_Stress_Level"]

    # # Split into training (80%) and testing (20%) datasets
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # # Initialize the model
    # model = RandomForestRegressor(n_estimators=100, random_state=42)

    # # Train the model
    # model.fit(X_train, y_train)
    # # Predict on test data
    # y_pred = model.predict(X_test)

    # # Evaluate performance
    # mae = mean_absolute_error(y_test, y_pred)
    # r2 = r2_score(y_test, y_pred)

    # print(f"Mean Absolute Error: {mae}")
    # print(f"R² Score: {r2}")
    # new_person = pd.DataFrame({
    #     "Years_Left": [25],
    #     "Physical_Activity_Score": [3],  # High activity
    #     "Sleep_Quality_Score": [2],      # Average sleep
    #     "Mental_Health_Access_Score": [3],  # Full access
    #     "Mental_Health_Condition_Score": [5]  # No mental health condition
    # })

    # predicted_stress = model.predict(new_person)
    # print(f"Predicted Future Stress Level: {predicted_stress[0]}")


def transform_data(raw_data_path, processed_data_path, columns_to_drop, rows_to_drop):
    df = pd.read_csv(raw_data_path)

    check_nan(df, processed_data_path)
    df_processed = drop_columns(df, columns_to_drop, processed_data_path)
    df_processed = drop_rows(df_processed, rows_to_drop, processed_data_path)
    generate_insights(df_processed, processed_data_path)
    # generate_insights_using_ML_model(df, processed_data_path)
