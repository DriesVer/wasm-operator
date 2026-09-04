pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit",
        world: "wasmoperator",
    });
}

use bindings::local::kube::api::{Error, HttpError};

impl From<kube::Error> for Error {
    fn from(err: kube::Error) -> Self {
        match err {
            kube::Error::Api(status) => {
                if status.code == 404 {
                    Error::NotFound
                } else {
                    Error::Http(HttpError {
                        code: status.code,
                        reason: status.reason,
                        message: status.message,
                    })
                }
            }
            other => Error::Other(other.to_string()),
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<kube::Error>() {
            Ok(kube_err) => kube_err.into(),
            Err(other) => Error::Other(other.to_string()),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Other(err.to_string())
    }
}
