/*
 * QkThings LICENSE
 * The open source framework and modular platform for smart devices.
 * Copyright (C) 2014 <http://qkthings.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef QKDAEMONTHREAD_H
#define QKDAEMONTHREAD_H

#include <QThread>
#include <QTcpSocket>

//class QkConnectionManager;
class QkDaemonSocket;
class QkAPIHandler;
class QkConnection;

class QkDaemonThread : public QThread
{
    Q_OBJECT
public:
    explicit QkDaemonThread(int socketDescriptor, QObject *parent = 0);

    QkDaemonSocket* socket();

protected:
    void run();
    
signals:
    void error(QTcpSocket::SocketError socketError);
    void clientConnected(int socketDescriptor);
    void clientDisconnected(int socketDescriptor);
    
public slots:
    void sendJson(const QJsonDocument &doc);

private slots:
    void _handleParsedJsonString(QString str);
    void _handleDisconnected();

private:
    void _setupConnections();

    int m_socketDescriptor;
    QkDaemonSocket *m_socket;
    //QkConnectionManager *m_connManager;
    QkAPIHandler *m_apiHandler;
    
};

#endif // QKDAEMONTHREAD_H
