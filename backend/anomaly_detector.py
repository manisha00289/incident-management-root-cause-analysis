
from sklearn.ensemble import RandomForestClassifier


def train_random_forest(X_train, y_train):
    """
    Train a Random Forest classifier for HDFS anomaly detection.
    """

    if len(X_train) == 0 or len(y_train) == 0:
        raise ValueError("Training data cannot be empty.")

    if len(X_train) != len(y_train):
        raise ValueError("Features and labels must have the same number of rows.")

    model = RandomForestClassifier(
        n_estimators=200,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    return model
