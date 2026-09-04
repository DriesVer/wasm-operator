//! # Prediction Module
//!
//! Handles communication with the prediction sidecar to forecast optimal loading
//! and unloading times for child Wasm operators based on their reconciliation history.

use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const PREDICTION_SERVICE_PORT: u16 = 5000;

#[allow(dead_code)]
#[derive(Serialize)]
/// Supported prediction models for reconcile forecasting.
pub enum PredictionModel {
    AutoReg,
    ARIMA,
    SARIME,
    SES,
    Holt,
    Winter,
}

#[derive(Serialize)]
/// Request payload for the prediction service.
struct PredictionRequest {
    history: Vec<DateTime<Utc>>,
    function: PredictionModel,
}

#[derive(Deserialize, Debug)]
/// Response from the prediction service containing the next prediction.
struct PredictionResponse {
    prediction: DateTime<Utc>,
}

/// Fetches the next reconcile prediction from the prediction service.
pub async fn get_next_reconcile_prediction(
    history: Vec<DateTime<Utc>>,
    model: PredictionModel,
) -> Result<DateTime<Utc>> {
    let client = Client::new();
    let url = format!("http://localhost:{}/prediction", PREDICTION_SERVICE_PORT);

    let request_payload = PredictionRequest {
        history,
        function: model,
    };

    // The parent operator sends a POST request when an operator becomes inactive
    let response = client
        .post(url)
        .json(&request_payload)
        .send()
        .await?
        .error_for_status()?
        .json::<PredictionResponse>()
        .await?;

    Ok(response.prediction)
}
