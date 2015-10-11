#include "qkconnmanager.h"
#include "qkdaemon_gui.h"

#include <QDebug>
#include <QUuid>

int QkConnManager::_nextPort = 1234;

QkConnManager::QkConnManager()
{
    _host = QHostAddress(QHostAddress::LocalHost);
}

bool QkConnManager::add(int connType, QMap<QString, QString> args, int *id)
{
    QkConnWorker *worker = new QkConnWorker(_nextPort, this);

    if(worker->open((QkConnWorker::ConnType)connType, args))
    {
        *id = qrand() & 0xFFFF;
        worker->setID(*id);
        connect(worker, SIGNAL(message(int,QString)),
                this, SLOT(_slotMessageSentByWorker(int,QString)));
        _connWorkers.insert(*id, worker);
        _nextPort += 2;
        return true;
    }

    delete worker;
    return false;
}

bool QkConnManager::remove(QString id)
{

}

QkConnWorker* QkConnManager::worker(int id)
{
    return _connWorkers.value(id);
}

void QkConnManager::_slotMessageSentByWorker(int type, QString msg)
{
    QkConnWorker *worker = (QkConnWorker *) sender();

    emit message(type, QString().sprintf("%04X ", worker->id()) + msg);
}

