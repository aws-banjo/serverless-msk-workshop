# Lambda base image
FROM public.ecr.aws/lambda/python:3.8
# Install Kafka prereqs
RUN yum -y install java-11 wget tar
RUN wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
RUN tar -xzf kafka_2.12-2.8.1.tgz
RUN wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.1/aws-msk-iam-auth-1.1.1-all.jar
RUN mv aws-msk-iam-auth-1.1.1-all.jar ${LAMBDA_TASK_ROOT}/kafka_2.12-2.8.1/libs
COPY ./client.properties ${LAMBDA_TASK_ROOT}
# Remove tgz
RUN rm kafka_2.12-2.8.1.tgz
RUN pip3 install pandas
# Lambda code
COPY handler.py ${LAMBDA_TASK_ROOT}
# Run handler
CMD ["handler.action"]