import aws_cdk as cdk
from constructs import Construct

"""
This test of CDK creates a new VPC and a pair of public and
private subnets
"""

class CDKStack(cdk.Stack):

    def __init__(self, scope:Construct, id: str, **kwargs)->None:

        super().__init__(scope, id, **kwargs)

        self.vpc = cdk.aws_ec2.Vpc(
            self,
            "IFC_Infra_VPC",
            max_azs=2,
            cidr="10.10.0.0/16",
            subnet_configuration=[
                cdk.aws_ec2.SubnetConfiguration(
                    subnet_type=cdk.aws_ec2.SubnetType.PUBLIC,
                    name="IFC_Public",
                    cidr_mask=24
                ),
                cdk.aws_ec2.SubnetConfiguration(
                    subnet_type=cdk.aws_ec2.SubnetType.PRIVATE_WITH_NAT,
                    name="IFC_Private",
                    cidr_mask=24
                ),
                cdk.aws_ec2.SubnetConfiguration(
                    subnet_type=cdk.aws_ec2.SubnetType.PRIVATE_ISOLATED,
                    name="IFC_Isolated",
                    cidr_mask=24
                )
            ],
            nat_gateways=2
        )

        cdk.CfnOutput(
            self,
            "Test_Output",
            value=self.vpc.vpc_id
        )

if __name__ == "__main__":
    
    app = cdk.App()
    vpc_stack = CDKStack(app, "TestVPC")

    print(app.to_string())