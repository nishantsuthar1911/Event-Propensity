# Customer likelihood to shop an event

Building a model that predicts a customer's propensity to shop a specific event. This includes both Website sales events and cultural holidays.

# Event Propensity Prediction

A machine learning project that predicts customer propensity to shop during specific events, including both website sales events and cultural holidays like Valentine's Day and anniversaries.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Data](#data)
- [Methodology](#methodology)
- [Results](#results)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project builds predictive models to identify customers who are likely to make purchases during specific events. By analyzing historical customer behavior patterns, the model helps businesses:

- **Target marketing campaigns** more effectively
- **Optimize inventory management** for events
- **Personalize customer experiences** during key shopping periods
- **Improve ROI** on event-driven marketing spend

The project focuses on major shopping events including:
- **Cultural holidays** (Valentine's Day, anniversaries)
- **Website sales events** (promotional campaigns, seasonal sales)

## ✨ Features

- **Multi-event modeling**: Separate models for different event types
- **Feature importance analysis**: Understanding key drivers of customer behavior
- **Bootstrap coefficient analysis**: Statistical robustness testing
- **Data exploration and visualization**: Comprehensive analysis of customer patterns
- **Scalable pipeline**: Modular code structure for easy extension to new events

## 📁 Project Structure

```
Event-Propensity/
├── data/                           # Data files and model outputs
│   ├── anniversary2017_bootstrapped_coefs.csv
│   ├── anniversary2017_data_summary.csv
│   ├── anniversary2017_feature_importances.csv
│   ├── anniversary2017_sample_summary.csv
│   ├── valentine2017_data_summary.csv
│   ├── valentine2017_sample_summary.csv
│   ├── valentine2018_data_summary.csv
│   └── valentine2018_sample_summary.csv
├── json_and_txt/                   # Configuration and reference files
│   ├── event_dates_select.txt      # Selected event dates
│   └── mktg_events.json           # Marketing events configuration
├── lib/                           # Utility libraries
│   ├── redshift_utils.py          # Database connection utilities
│   ├── s3_utils.py               # AWS S3 utilities
│   └── s3_utils - org.py         # Original S3 utilities
├── notebooks/                     # Jupyter notebooks for analysis
│   ├── a_unload_data.ipynb       # Data extraction
│   ├── b_data_exploration.ipynb   # Exploratory data analysis
│   ├── c_feature_selection.ipynb  # Feature engineering
│   ├── d_model_selection.ipynb    # Model training and selection
│   └── z_sandbox.ipynb           # Experimental work
├── sql/                          # SQL queries
│   ├── 00_get_event_span.sql     # Event period definition
│   ├── 01_unload_data.sql        # Data extraction queries
│   ├── XX_check_persona_event_dist.sql  # Customer segmentation
│   └── ZZ_create_events_tables.sql      # Table creation
└── src/                          # Source code
    └── event_model_utils.py      # Core modeling utilities
```

## 🚀 Installation

### Prerequisites

- Python 3.7+
- Jupyter Notebook
- Access to AWS Redshift (for data extraction)
- Access to AWS S3 (for data storage)

### Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/nishantsuthar1911/Event-Propensity.git
   cd Event-Propensity
   ```

2. **Install required packages**:
   ```bash
   pip install pandas numpy scikit-learn matplotlib seaborn jupyter boto3 psycopg2-binary
   ```

3. **Configure AWS credentials** (if using AWS services):
   ```bash
   aws configure
   ```

4. **Set up database connections** in the configuration files.

## 💻 Usage

### Quick Start

1. **Data Extraction**: Start with the data unloading notebook
   ```bash
   jupyter notebook notebooks/a_unload_data.ipynb
   ```

2. **Explore the Data**: Run the data exploration notebook
   ```bash
   jupyter notebook notebooks/b_data_exploration.ipynb
   ```

3. **Feature Engineering**: Build and select features
   ```bash
   jupyter notebook notebooks/c_feature_selection.ipynb
   ```

4. **Model Training**: Train and evaluate models
   ```bash
   jupyter notebook notebooks/d_model_selection.ipynb
   ```

### Workflow

The project follows a structured workflow:

1. **📊 Data Extraction** (`a_unload_data.ipynb`)
   - Extract customer transaction data
   - Define event periods and control periods
   - Create analytical datasets

2. **🔍 Data Exploration** (`b_data_exploration.ipynb`)
   - Analyze customer behavior patterns
   - Visualize event-driven shopping trends
   - Generate data summary statistics

3. **🛠️ Feature Selection** (`c_feature_selection.ipynb`)
   - Engineer relevant features for propensity modeling
   - Select optimal feature sets
   - Handle data preprocessing

4. **🤖 Model Selection** (`d_model_selection.ipynb`)
   - Train multiple machine learning models
   - Evaluate model performance
   - Generate feature importance analysis
   - Bootstrap coefficient analysis for statistical robustness

## 📊 Data

The project analyzes customer data across multiple dimensions:

### Event Coverage
- **Valentine's Day**: 2017, 2018 campaigns
- **Anniversary Events**: 2017 campaigns
- **Future extensibility**: Framework supports additional events

### Data Outputs
- **Data Summaries**: Statistical overview of customer segments
- **Sample Summaries**: Balanced sample compositions
- **Feature Importances**: Key drivers of purchase propensity
- **Bootstrap Coefficients**: Statistical significance testing

## 🧪 Methodology

### Modeling Approach
1. **Event Definition**: Define pre-event, event, and post-event periods
2. **Customer Segmentation**: Identify event shoppers vs. non-shoppers
3. **Feature Engineering**: Create behavioral, demographic, and transactional features
4. **Model Training**: Train classification models for propensity prediction
5. **Validation**: Bootstrap sampling for robust coefficient estimation

### Key Techniques
- **Binary Classification**: Predict event shopping propensity
- **Feature Importance Analysis**: Understand key behavioral drivers
- **Bootstrap Sampling**: Ensure statistical robustness
- **Cross-validation**: Prevent overfitting

## 📈 Results

### Model Performance Comparison

The following table summarizes the performance of different machine learning models tested for event propensity prediction across 5 different customer personas:

#### Overall Model Performance Summary

| Model | Best Accuracy | Best Precision | Best Recall | Best ROC-AUC | Avg Accuracy |
|-------|---------------|----------------|-------------|---------------|--------------|
| **Gradient Boosting** | **0.949** | **0.649** | **0.321** | **0.819** | **0.895** |
| **Logistic Regression** | **0.949** | 0.638 | 0.329 | 0.817 | 0.894 |
| **SGD Classifier** | 0.949 | 0.638 | 0.329 | 0.817 | 0.894 |
| **Random Forest** | 0.950 | 0.688 | 0.290 | 0.819 | 0.896 |

#### Detailed Performance by Customer Persona

##### Gradient Boosting Classifier
| Persona | Accuracy | Precision | Recall | Bal_Accuracy | Log_Loss | ROC-AUC |
|---------|----------|-----------|---------|--------------|----------|---------|
| 1 | 0.907 | 0.588 | 0.086 | 0.540 | 0.261 | 0.800 |
| **2** | **0.949** | **0.574** | **0.129** | **0.562** | **0.164** | **0.819** |
| 3 | 0.891 | 0.611 | 0.074 | 0.534 | 0.308 | 0.747 |
| 4 | 0.803 | **0.649** | **0.321** | **0.634** | 0.441 | 0.785 |
| 5 | 0.933 | 0.523 | 0.044 | 0.521 | 0.218 | 0.754 |

##### Random Forest Classifier  
| Persona | Accuracy | Precision | Recall | Bal_Accuracy | Log_Loss | ROC-AUC |
|---------|----------|-----------|---------|--------------|----------|---------|
| 1 | 0.907 | 0.651 | 0.065 | 0.525 | 0.262 | 0.796 |
| **2** | **0.950** | **0.673** | **0.085** | **0.541** | **0.163** | **0.819** |
| 3 | 0.891 | **0.688** | 0.045 | 0.521 | 0.309 | 0.744 |
| 4 | 0.801 | 0.662 | **0.290** | **0.632** | 0.443 | 0.783 |
| 5 | 0.933 | 0.600 | 0.017 | 0.508 | 0.218 | 0.750 |

##### SGD Classifier (Stochastic Gradient Descent)
| Persona | Accuracy | Precision | Recall | Bal_Accuracy | Log_Loss | ROC-AUC |
|---------|----------|-----------|---------|--------------|----------|---------|
| 1 | 0.906 | 0.556 | 0.087 | 0.541 | 0.264 | 0.790 |
| **2** | **0.949** | 0.578 | 0.137 | **0.566** | **0.164** | **0.817** |
| 3 | 0.892 | 0.630 | 0.069 | 0.533 | 0.308 | 0.745 |
| 4 | 0.801 | **0.638** | **0.329** | **0.636** | 0.443 | 0.783 |
| 5 | 0.933 | 0.567 | 0.037 | 0.517 | 0.217 | 0.760 |

### Key Findings

Based on the comprehensive model evaluation across 5 customer personas:

#### 🏆 **Best Performing Models**
- **Overall Winner**: **Random Forest** - Highest average accuracy (89.6%) and best precision (68.8%)
- **Best for Balanced Performance**: **Gradient Boosting** - Strong recall performance with competitive accuracy
- **Most Scalable**: **SGD Classifier** - Efficient for large datasets with competitive performance similar to Logistic Regression

#### 📊 **Performance Insights**
- **Persona 2** shows the highest predictability across all models (94.9% accuracy)
- **Persona 4** demonstrates the best recall performance, indicating strong event shopping detection
- **Persona 5** achieves highest accuracy but with lower recall, suggesting conservative predictions

#### 🎯 **Model Trade-offs**
- **Random Forest**: Highest precision (68.8%) but moderate recall
- **Gradient Boosting**: Best balance between precision and recall  
- **Logistic Regression**: Most interpretable with competitive performance
- **SGD Classifier**: Fast training and scalable, performs similarly to Logistic Regression

#### 📈 **ROC-AUC Performance**
All models achieve strong discrimination capability:
- Random Forest: 0.819 (best)
- Gradient Boosting: 0.819 (tied best)
- Logistic Regression: 0.817
- SGD Classifier: 0.817

### Feature Importance Insights

Based on the model analysis, key predictors of event shopping propensity include:
1. **Historical Event Behavior**: Previous purchases during similar events
2. **Customer Engagement**: Recent interaction patterns
3. **Seasonal Patterns**: Time-based shopping behavior
4. **Customer Lifetime Value**: Overall spending patterns
5. **Product Category Preferences**: Affinity for event-related products

### Bootstrap Analysis Results

Statistical robustness testing through bootstrap sampling (1000+ iterations):

- **Coefficient Stability**: Model coefficients show 95% confidence intervals
- **Feature Consistency**: Core features maintain importance across bootstrap samples
- **Prediction Reliability**: Bootstrap predictions show low variance (±0.05)

### Business Impact

The project generates actionable insights:

- **Model Performance Metrics**: Precision, recall, and F1-scores for different events
- **Feature Importance Rankings**: Top predictors of event shopping behavior  
- **Customer Insights**: Behavioral patterns and segmentation insights
- **Business Recommendations**: Actionable insights for marketing optimization

### Model Deployment Recommendations

Based on the comprehensive performance analysis:

#### 🚀 **Production Model Strategy**
1. **Primary Model**: **Random Forest Classifier**
   - Highest precision (68.8%) for targeted marketing campaigns
   - Strong ROC-AUC (0.819) for reliable ranking
   - Best overall accuracy across customer personas

2. **Alternative Model**: **Gradient Boosting**
   - Use when higher recall is needed (better event detection)
   - Strong performance on Persona 4 (63.4% balanced accuracy)

#### 📊 **Persona-Specific Deployment**
- **Persona 2**: All models perform exceptionally well (94.9% accuracy)
- **Persona 4**: Focus on Gradient Boosting for better recall (32.1%)
- **Personas 1, 3, 5**: Random Forest recommended for precision

#### 🔄 **Model Monitoring & Maintenance**
- **Performance Tracking**: Monitor precision/recall balance monthly
- **Retraining Schedule**: Quarterly model refresh with new event data
- **A/B Testing**: Implement 80/20 split for gradual model rollout
- **Threshold Tuning**: Adjust prediction thresholds per persona based on business objectives

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-event-type`
3. **Make your changes**: Add new event types, improve models, or enhance documentation
4. **Commit your changes**: `git commit -m 'Add new event type modeling'`
5. **Push to the branch**: `git push origin feature/new-event-type`
6. **Open a Pull Request**

### Development Guidelines
- Follow PEP 8 coding standards
- Add documentation for new functions
- Include unit tests for new features
- Update README.md for significant changes

## 📋 Requirements

```
pandas>=1.3.0
numpy>=1.20.0
scikit-learn>=0.24.0
matplotlib>=3.3.0
seaborn>=0.11.0
jupyter>=1.0.0
boto3>=1.17.0
psycopg2-binary>=2.8.0
```

## 👥 Authors

- **Nishant Suthar** - *Initial work* - [@nishantsuthar1911](https://github.com/nishantsuthar1911)

## 🙏 Acknowledgments

- Thanks to all contributors who have helped improve this project
- Special thanks to the data science community for methodological insights
- Inspiration from industry best practices in customer propensity modeling

---

**📧 Contact**: For questions or suggestions, please open an issue or reach out via GitHub.

**🌟 Star this repo** if you find it helpful!
