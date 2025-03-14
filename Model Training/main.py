from model import Model

def train_model(data_path, label_column, drop_columns, model_dir, artifacts_dir):
    xtrain, xtest, ytrain, ytest = Model.load_data(data_path=data_path,
                                                   label_column=label_column, drop_columns=drop_columns)
    model, report = Model.train(xtrain, xtest, ytrain, ytest)
    Model.save_model(model_dir=model_dir,
                     artifacts_dir=artifacts_dir,
                     model=model, report=report)

if __name__ == "__main__":
    xtrain, xtest, ytrain, ytest = Model.load_data(data_path="../Dataset/Processed Data/processed_data.csv",
                                                   label_column="Churn", drop_columns=["Churn", "customerID"])
    model, report = Model.train(xtrain, xtest, ytrain, ytest)
    Model.save_model(model_dir="../Models/Customer Churn/models",
                     artifacts_dir="../Models/Customer Churn/artifacts",
                     model=model, report=report)