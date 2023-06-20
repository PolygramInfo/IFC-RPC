# Setup CDK Stack

## 1. Install CDK

1. Check calling identity `aws sts get-caller-identity`
    * Configure aws CLI `aws configure` if the incorrect/no identity
2. Check installed CDK version `cdk --version`
    * If this is unsuccessful install CDK `npm install -g aws-cdk`
3. Install/reinstall CDK `python -m pip install aws-cdk` or `python -m pip install aws-cdk-lib`
4. Run init in an empty infrastructure folder `cdk init`
5. Begin building a stack.