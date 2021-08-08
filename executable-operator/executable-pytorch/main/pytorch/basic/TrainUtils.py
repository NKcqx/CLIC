import torch
import time
from executable.basic.utils.Logger import Logger
"""
Time       : 2021/7/19 2:15 下午
Author     : zjchen
Description:
"""

trainLogger = Logger('TrainLogger').logger


def train(train_iter, net, loss, optimizer, tol_threshold, num_epochs):
    """
    Description: 用于训练网络的基本函数
    Returns: 训练结束后的网络模型
    """
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    net = net.to(device)
    # trainLogger.info("training on ", device)
    batch_count = 0
    last_loss = 0
    for epoch in range(num_epochs):
        train_l_sum, train_acc_sum, n, start = 0.0, 0.0, 0, time.time()
        for X, y in train_iter:
            X = X.to(device)
            y = y.to(device)
            y_hat = net(X)
            l = loss(y_hat, y)
            optimizer.zero_grad()
            l.backward()
            optimizer.step()
            train_l_sum += l.cpu().item()
            train_acc_sum += (y_hat.argmax(dim=1) == y).sum().cpu().item()
            n += y.shape[0]
            batch_count += 1
            # if test_iter is not None:
            #     train_l_sum += l.cpu().item()
            #     train_acc_sum += (y_hat.argmax(dim=1) == y).sum().cpu().item()
            #     n += y.shape[0]
            #     batch_count += 1
        cur_loss = train_l_sum / batch_count
        tol = abs(cur_loss - last_loss)
        trainLogger.info('epoch %d, loss %.4f, tol = %.4f,train acc %.3f, time %.1f sec'
                         % (epoch + 1, train_l_sum / batch_count, tol, train_acc_sum / n, time.time() - start))
        if tol < tol_threshold:
            break
        else:
            last_loss = cur_loss
        # if test_iter is not None:
        #     test_acc = evaluate_accuracy(test_iter, net)
        #     print('epoch %d, loss %.4f, train acc %.3f, test acc %.3f, time %.1f sec'
        #           % (epoch + 1, train_l_sum / batch_count, train_acc_sum / n, test_acc, time.time() - start))
    return net


def evaluate_accuracy(data_iter, net, device=None):
    if device is None and isinstance(net, torch.nn.Module):
        # 如果没指定device就使用net的device
        device = list(net.parameters())[0].device
    acc_sum, n = 0.0, 0
    with torch.no_grad():
        for X, y in data_iter:
            if isinstance(net, torch.nn.Module):
                net.eval()  # 评估模式, 这会关闭dropout
                acc_sum += (net(X.to(device)).argmax(dim=1) == y.to(device)).float().sum().cpu().item()
                net.train()  # 改回训练模式
            else:  # 自定义的模型, 3.13节之后不会用到, 不考虑GPU
                if ('is_training' in net.__code__.co_varnames):  # 如果有is_training这个参数
                    # 将is_training设置成False
                    acc_sum += (net(X, is_training=False).argmax(dim=1) == y).float().sum().item()
                else:
                    acc_sum += (net(X).argmax(dim=1) == y).float().sum().item()
            n += y.shape[0]
    return acc_sum / n
