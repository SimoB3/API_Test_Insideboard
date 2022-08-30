HOST_PORT=8080
IMAGE_NAME=insideboard-test-api
CONTAINER_NAME=${IMAGE_NAME}

set -e
trap "docker stop ${CONTAINER_NAME} > /dev/null && printf '\n=> API stopped <='" EXIT


printf '\n=== Building API... ===\n\n'
docker build -t ${IMAGE_NAME} .
printf '\n=== API built ! ===\n'

printf '\nStarting API...\n'
docker run -d --name ${CONTAINER_NAME} -p ${HOST_PORT}:80 --rm ${IMAGE_NAME} > /dev/null
printf "=> API serving on port ${HOST_PORT} !\n"

printf "\n=> You can get the API logs using 'docker logs -f ${CONTAINER_NAME}'"
printf "\n=> You can get the API documentation at 'localhost:8080/docs'"
printf "\n=> You can stop the API with Ctrl+C\n\n"

exit_code="$(docker container wait ${CONTAINER_NAME})"
printf "\nAPI exited with code: ${exit_code}\n\n"