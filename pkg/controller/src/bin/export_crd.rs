use controller::kubernetes::crd::WasmOperator;
use kube::CustomResourceExt;

/// Generates and prints the Kubernetes Custom Resource Definition (CRD) of the WasmOperator CRD as YAML.
fn main() {
    // This generates the YAML as a String
    let crd = WasmOperator::crd();

    // Print it to stdout or write to a file
    println!("{}", serde_yaml::to_string(&crd).unwrap());
}
