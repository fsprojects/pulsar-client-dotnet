

kubectl create namespace pulsartest

kubectl config set-context --current --namespace=pulsartest

kubectl apply -f . -n pulsartest 
