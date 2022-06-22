# IBM License Usage Aggregator for Fargate

## Scope

AWS ECS Fargate technology allows users to deploy containers in serverless environment and the license of such software needs to be tracked.
In case of IBM Certified Containers, license is calculated using [IBM Container Pricing](https://www.ibm.com/software/passportadvantage/containerlicenses.html)
rules and proper license usage tracing tool needs to be used to have license
tracked and that can be achieved with IBM License Usage Logger for Fargate sidecar.
This repo provides an example of a tool for aggregation of such license usage logs.
This will allow customers to have better understanding by getting daily metrics values for given IBM products based on provided license usage logs.

## Requirements

1. [AWS cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. [Python 3](https://www.python.org/downloads/)
3. License Service S3 bucket content from your AWS environment

## Usage

1. Copy `IBM-License-usage` folder from given License Service S3 bucket to local directory:

   ```aws s3 cp s3://<S3_bucket_name>/IBM-License-usage <s3_license_usage_directory> --recursive```

1. Run aggregator by

   (Optional) Enable DEBUG mode by setting DEBUG = True inside the script.

   ```python scripts/IBM_license_usage_aggregator_for_fargate.py <s3_license_usage_directory> <output_directory>```

1. Get csv files with daily aggregated license usage from the `output-directory`

## Notes

**NOTE: This repository has been configured with the [DCO bot](https://github.com/probot/dco).**

Pull requests are very welcome! Make sure your patches are well tested.
Ideally create a topic branch for every separate change you make. For
example:

1. Fork the repo
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Commit your changes (`git commit -am 'Added some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create new Pull Request

## License

All source files must include a Copyright and License header. The SPDX license header is
preferred because it can be easily scanned.

If you would like to see the detailed LICENSE click [here](LICENSE).

```text
#
# Copyright 2022 IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
```
