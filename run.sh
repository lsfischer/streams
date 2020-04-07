docker pull nunopreguica/ps1819
docker run --network=ps-net -p 8888:8888 -v $(pwd)/log:/data -v $(pwd)/notebooks:/home/jovyan/work nunopreguica/ps1819
