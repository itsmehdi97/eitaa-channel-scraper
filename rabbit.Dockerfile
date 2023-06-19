FROM rabbitmq:latest

RUN echo "management_agent.disable_metrics_collector = false" > /etc/rabbitmq/conf.d/management_agent.disable_metrics_collector.conf
RUN rabbitmq-plugins enable rabbitmq_management