make kube-scheduler
docker rmi my-kube-scheduler:1.22
docker build -t my-kube-scheduler:1.22 .
docker exec godel-demo-default-worker3 crictl rmi my-kube-scheduler:1.22
docker exec godel-demo-default-worker crictl rmi my-kube-scheduler:1.22
docker exec godel-demo-default-control-plane crictl rmi my-kube-scheduler:1.22
docker exec godel-demo-default-worker2 crictl rmi my-kube-scheduler:1.22
kind load docker-image my-kube-scheduler:1.22 --name godel-demo-default
kubectl delete deployment my-scheduler -n kube-system
kubectl apply -f ../my-scheduler.yaml
kubectl delete pod annotation-second-scheduler -n kube-system
kubectl apply -f ../my-pod.yaml
kubectl delete pod annotation-third-scheduler -n kube-system
kubectl apply -f ../pod2.yaml