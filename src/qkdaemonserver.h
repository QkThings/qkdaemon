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

#ifndef QKDAEMONSERVER_H
#define QKDAEMONSERVER_H

#include <QTcpServer>
#include <QMap>

class QkDaemonThread;
class QkDaemonSocket;
class QkConnectionManager;

class QkDaemonServer : public QTcpServer
{
    Q_OBJECT
public:
    explicit QkDaemonServer(QkConnectionManager *connManager, QObject *parent = 0);
    
protected:
    void incomingConnection(qintptr socketDescriptor);

signals:
    void info(QString message);
    
public slots:

private slots:
    void _handleClientConnected(int socketDescriptor);
    void _handleClientDisconnected(int socketDescriptor);

private:
    QMap<int, QkDaemonThread*> m_threads;
    QkConnectionManager *m_connManager;
    
};

#endif // QKDAEMONSERVER_H
