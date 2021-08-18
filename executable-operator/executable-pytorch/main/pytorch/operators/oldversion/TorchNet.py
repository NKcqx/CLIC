from executable.basic.model.OperatorBase import OperatorBase
from pytorch.basic.LossFunctionFactory import LossFunctionFactory
from pytorch.basic.OptimizerFactory import OptimizerFactory
from torch.optim.lr_scheduler import StepLR
from torchvision import transforms
import importlib
import torch

"""
@ProjectName: CLIC
@Time       : 2020/11/29 上午10:20
@Author     : zjchen, NKCqx
@Description: 一个pytorch通用网络结构
"""


class TorchNet(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("TorchNet", ID, inputKeys, outputKeys, Params)

    def train(self, model, loss_function, train_loader, optimizer, epoch):
        model.train()
        for batch_idx, (data, target) in enumerate(train_loader):
            data, target = data.to(self.params["device"]), target.to(self.params["device"])
            optimizer.zero_grad()
            output = model(data)
            loss = loss_function(output, target)
            loss.backward()
            optimizer.step()
            if batch_idx % self.params["log-interval"] == 0:
                logger.info('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                    epoch, batch_idx, batch_idx * len(data), len(train_loader.dataset),
                    100. * batch_idx / len(train_loader), loss.item()))
                # if self.params["dry_run"]:
                #     break

    def test(self, model, loss_function, test_loader):
        model.eval()
        test_loss = 0
        correct = 0
        with torch.no_grad():
            for data, target in test_loader:
                data, target = data.to(self.params["device"]), target.to(self.params["device"])
                output = model(data)
                test_loss += loss_function(output, target, reduction='sum').item()  # sum up batch loss
                pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
                correct += pred.eq(target.view_as(pred)).sum().item()

        test_loss /= len(test_loader.dataset)

        # logger.info('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        #     test_loss, correct, len(test_loader.dataset),
        #     100. * correct / len(test_loader.dataset)))

    def execute(self):
        lossFunctionFactory = LossFunctionFactory()
        optimizerFactory = OptimizerFactory()
        kwargs = {'batch_size': self.params["batch-size"]}
        if self.params["device"] == "cuda":
            cuda_kwargs = {'num_workers': 1,
                           'pin_memory': True,
                           'shuffle': True}
            kwargs.update(cuda_kwargs)

        module = importlib.import_module(self.params["network"])
        # if isinstance(self.getInputData("train-data"), datasets):

        model = module.Net().to(self.params["device"])

        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,))
            ])
        data = module.Dataset(root=self.params["data-path"], train=self.params["train"], transform=transform)
        data_loader = torch.utils.data.DataLoader(data, **kwargs)
        loss_function = lossFunctionFactory.createLossFunction(self.params["loss_function"])
        optimizer = optimizerFactory.createOptimizer(self.params["optimizer"])
        if self.params["train"]:
            optimizer = optimizer(model.parameters(), lr=self.params["lr"])  # TODO: 可指定不同的optimizer 下同
            scheduler = StepLR(optimizer, step_size=1, gamma=self.params["gamma"])
            for epoch in range(1, self.params["epochs"] + 1):
                self.train(model, loss_function, data_loader, optimizer, epoch)
                scheduler.step()
        else:
            self.test(model, loss_function, data_loader)

