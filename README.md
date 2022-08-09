## Green Box Data Quality

This tool was developed to make easy to run Data Quality checks agains our pipelines.

It's development environment was a docker container with the setup exactly as described here:
https://github.com/digital-land/digital-land-docker-pipeline-runner

The unit tests expect the tool to be running from the following dir inside the container:
    
    /src/sharing_area/green-box-data-quality/

To make it easier to run the unit tests we suggest:

    export PYTHONPATH=/src/sharing_area/green-box-data-quality/

This is a very early stage of the tool and several paths are hardcoded to their location inside the container mentioned above. This will be reviewed later.
