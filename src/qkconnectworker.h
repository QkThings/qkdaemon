#ifndef QKCONNECTWORKER_H
#define QKCONNECTWORKER_H

#include <QObject>
#include <QStringList>
#include <QProcess>

class QkConnectWorker : public QObject
{
    Q_OBJECT
public:
    enum Task
    {
        taskVersion,
        taskListSerial
    };

    explicit QkConnectWorker(QObject *parent = 0);

signals:
    void availableSerialPorts(QStringList);
    void message(int,QString);

public slots:
    void version();
    void listSerial();

private slots:
    void _slotProcessFinished(int status);
    void _slotProcessError(QProcess::ProcessError error);

private:
    QProcess *_process;
    Task _currentTask;

    void _processWait();

};

#endif // QKCONNECTWORKER_H
