import json
import logging

from catboost import CatBoostClassifier, Pool
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

model = CatBoostClassifier()

model_th = 0.95
logger.info('Pretrained model imported successfully...')

def make_pred(dt, cat_features, source_info="kafka"):
    model.load_model('./models/model_catboost.cbm')
    test_pool = Pool(dt, cat_features=cat_features)
    
    probs = model.predict_proba(test_pool)[:, 1]
    submission = pd.DataFrame({
        'score': probs,
        'fraud_flag': (probs > model_th).astype(int)
    })

    logger.info(f'Prediction complete for data from {source_info}')
    return submission


def save_feature_importance_json(output_dir, filename, top_n=5):
    importances = model.get_feature_importance()
    features = model.feature_names_
    feat_imp = dict(zip(features, importances))
    top_features = dict(sorted(feat_imp.items(), key=lambda x: x[1], reverse=True)[:top_n])

    with open(f"{output_dir}/{filename}_top_features.json", "w") as f:
        json.dump(top_features, f)


def save_prediction_density_plot(preds_array, output_dir, filename):
    plt.figure(figsize=(6,4))
    plt.title("Плотность распределения предсказаний")
    plt.xlabel("Предсказанная вероятность")
    plt.ylabel("Плотность")
    plt.hist(preds_array, bins=50, density=True, alpha=0.7)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{filename}_pred_density.png")
    plt.close()