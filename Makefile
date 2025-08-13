PROJECT_NAME = glue-script-02
WD=$(shell pwd)
WORKSPACE_LOCATION=${WD}
PYTHON_INTERPRETER = python3
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

run_glue_image:
	@echo ">>> Running Glue script in Docker container..."
	@echo ">>> Current working directory: $(WD)"
	@echo ">>> Setting up environment variables..."
	
	docker run -it --rm \
	-v ~/.aws:/home/hadoop/.aws \
	-v $(WORKSPACE_LOCATION)/glu_script:/home/hadoop/workspace/ --user ${uid}:${gid} \
	-e AWS_PROFILE=$(PROFILE) \
	--name glue5_pyspark \
	public.ecr.aws/glue/aws-glue-libs:5 \