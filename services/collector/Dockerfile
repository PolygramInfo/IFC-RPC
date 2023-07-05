FROM public.ecr.aws/lambda/python:3.10

ENV AWS_REGION=us-east-1
ENV AWS_ACCESS_KEY_ID=<<ACCESS_KEY_ID>>
ENV AWS_SECRET_ACCESS_KEY=<<SECRET_ACCESS_KEY>>

COPY requirements.txt .
RUN pip3 install -r ./requirements.txt --target "${LAMBDA_TASK_ROOT}"
ADD sdk ${LAMBDA_TASK_ROOT}/sdk
COPY ./services/collector/router/app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.lambda_handler" ]
