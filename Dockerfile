FROM meltano/meltano:latest-python3.12

WORKDIR /project

# Install some useful tools
RUN apt-get update && \
    apt-get install -y jq curl inetutils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy over Meltano project directory
COPY . .

# Install Meltano plugins (tap-nhl, target-postgres)
RUN meltano install

# Prevent runtime modifications
ENV MELTANO_PROJECT_READONLY=1

ENTRYPOINT ["meltano"]
CMD ["run", "--full-refresh", "tap-nhl", "target-postgres"]